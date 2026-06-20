package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============== CONFIGURATION ==============

type ProducerConfig struct {
	BootstrapServers string
	// Default topic. Required unless TopicRouter is set and covers
	// every eventType you'll produce.
	Topic string
	// TopicRouter, if set, picks a topic per event type. Return "" to
	// fall back to Topic. Useful for routing different event types to
	// different topics from a single Producer.
	TopicRouter func(eventType string) string

	EnableLogging bool
	LogOutput     io.Writer
	ErrOutput     io.Writer

	// WriteTimeout bounds how long a single synchronous write waits for
	// a broker ack (also used as the worker-side timeout for async
	// sends if the caller's ctx has no deadline).
	WriteTimeout time.Duration
	// MaxAttempts is kafka-go's internal retry count for transient
	// write errors (e.g. leader-not-available during a rebalance).
	MaxAttempts int
	// RequiredAcks controls durability vs latency. Defaults to
	// RequireAll (wait for all in-sync replicas) -- safe default for a
	// system that also runs a DLQ-backed consumer on the other end.
	RequiredAcks kafka.RequiredAcks
	// BatchSize / BatchTimeout tune the writer's internal batching.
	BatchSize    int
	BatchTimeout time.Duration
	// Compression, e.g. kafka.Snappy. Zero value = none.
	Compression kafka.Compression
	// AllowAutoTopicCreation should generally stay false in production;
	// typo'd topic names should fail loudly, not silently create topics.
	AllowAutoTopicCreation bool

	// AsyncQueueSize bounds the buffered channel ProduceAsync writes
	// into. Once full, ProduceAsync blocks (backpressure) rather than
	// drop messages or grow memory unboundedly. Defaults to 1000.
	AsyncQueueSize int
	// AsyncWorkers is the number of goroutines draining the async
	// queue and performing the actual writes. Defaults to 4.
	AsyncWorkers int

	// Health threshold, same semantics as the consumer's: IsHealthy
	// flips to false once failures-in-window >= UnhealthyFailureThreshold.
	UnhealthyFailureThreshold int
	HealthFailureWindow       time.Duration
}

// ============== IMPLEMENTATION ==============

type asyncJob struct {
	ctx       context.Context
	key       string
	eventType string
	payload   []byte
	callback  func(error)
}

type KafkaProducer struct {
	config ProducerConfig
	writer *kafka.Writer

	logger    *log.Logger
	errLogger *log.Logger

	metrics *ProducerMetrics

	asyncCh    chan asyncJob
	asyncWG    sync.WaitGroup // counts queued + in-flight async jobs, for Flush
	workerWG   sync.WaitGroup // counts running worker goroutines, for Close
	shutdownCh chan struct{}
	closed     atomic.Bool
}

var _ Producer = (*KafkaProducer)(nil)

func NewKafkaProducer(cfg ProducerConfig) (*KafkaProducer, error) {
	if cfg.BootstrapServers == "" {
		return nil, errors.New("BootstrapServers must be specified")
	}
	if cfg.Topic == "" && cfg.TopicRouter == nil {
		return nil, errors.New("either Topic or TopicRouter must be specified")
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.ErrOutput == nil {
		cfg.ErrOutput = os.Stderr
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RequiredAcks == 0 {
		cfg.RequiredAcks = kafka.RequireAll
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 50 * time.Millisecond
	}
	if cfg.AsyncQueueSize <= 0 {
		cfg.AsyncQueueSize = 1000
	}
	if cfg.AsyncWorkers <= 0 {
		cfg.AsyncWorkers = 4
	}
	if cfg.UnhealthyFailureThreshold <= 0 {
		cfg.UnhealthyFailureThreshold = 10
	}
	if cfg.HealthFailureWindow <= 0 {
		cfg.HealthFailureWindow = time.Minute
	}

	w := &kafka.Writer{
		Addr: kafka.TCP(cfg.BootstrapServers),
		// Topic is intentionally left unset on the writer: per-message
		// Topic (set in publish()) is required to make TopicRouter work,
		// and kafka-go disallows setting both Writer.Topic and
		// Message.Topic at once.
		Balancer:               &kafka.Hash{}, // key-based partitioning -> same key always lands on the same partition
		MaxAttempts:            cfg.MaxAttempts,
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           cfg.BatchTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		RequiredAcks:           cfg.RequiredAcks,
		Compression:            cfg.Compression,
		AllowAutoTopicCreation: cfg.AllowAutoTopicCreation,
		Async:                  false, // we implement our own async layer above this, so the writer itself stays simple/synchronous
	}

	if cfg.EnableLogging {
		w.Logger = log.New(cfg.LogOutput, "[KAFKA_WRITER] ", log.LstdFlags)
		w.ErrorLogger = log.New(cfg.ErrOutput, "[KAFKA_WRITER_ERR] ", log.LstdFlags)
	}

	p := &KafkaProducer{
		config:     cfg,
		writer:     w,
		logger:     log.New(cfg.LogOutput, "[KAFKA_PRODUCER] INFO: ", log.LstdFlags),
		errLogger:  log.New(cfg.ErrOutput, "[KAFKA_PRODUCER] ERROR: ", log.LstdFlags),
		metrics:    &ProducerMetrics{StartTime: time.Now()},
		asyncCh:    make(chan asyncJob, cfg.AsyncQueueSize),
		shutdownCh: make(chan struct{}),
	}

	for i := 0; i < cfg.AsyncWorkers; i++ {
		p.workerWG.Add(1)
		go p.asyncWorker()
	}

	return p, nil
}

// ============== PUBLIC METHODS ==============

func (p *KafkaProducer) Produce(ctx context.Context, eventType string, data interface{}) error {
	return p.ProduceKey(ctx, eventType, eventType, data)
}

func (p *KafkaProducer) ProduceKey(ctx context.Context, key string, eventType string, data interface{}) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}

	payload, err := marshalEvent(eventType, data)
	if err != nil {
		return fmt.Errorf("messaging: marshal event %q: %w", eventType, err)
	}

	if err := p.publish(ctx, key, eventType, payload); err != nil {
		p.metrics.recordError(err)
		p.logErr("Failed to produce event %s: %v", eventType, err)
		return err
	}

	p.metrics.recordSuccess()
	p.logInfo("Produced event %s (key=%s)", eventType, key)
	return nil
}

func (p *KafkaProducer) ProduceAsync(ctx context.Context, key string, eventType string, data interface{}, callback func(error)) {
	if p.closed.Load() {
		if callback != nil {
			callback(ErrProducerClosed)
		}
		return
	}

	payload, err := marshalEvent(eventType, data)
	if err != nil {
		wrapped := fmt.Errorf("messaging: marshal event %q: %w", eventType, err)
		p.metrics.recordError(wrapped)
		if callback != nil {
			callback(wrapped)
		}
		return
	}

	job := asyncJob{ctx: ctx, key: key, eventType: eventType, payload: payload, callback: callback}

	p.asyncWG.Add(1)
	select {
	case p.asyncCh <- job:
		// enqueued; asyncWG.Done() happens in the worker once processed
	case <-ctx.Done():
		p.asyncWG.Done()
		if callback != nil {
			callback(ctx.Err())
		}
	case <-p.shutdownCh:
		p.asyncWG.Done()
		if callback != nil {
			callback(ErrProducerClosed)
		}
	}
}

func (p *KafkaProducer) Flush(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		p.asyncWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *KafkaProducer) Health() ProducerHealth {
	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	lastErr := ""
	if p.metrics.LastError != nil {
		lastErr = p.metrics.LastError.Error()
	}

	return ProducerHealth{
		IsHealthy:          p.metrics.recentFailureCountLocked(p.config.HealthFailureWindow) < p.config.UnhealthyFailureThreshold,
		MessagesSent:       atomic.LoadInt64(&p.metrics.MessagesSent),
		MessagesFailed:     atomic.LoadInt64(&p.metrics.MessagesFailed),
		AsyncQueueLength:   len(p.asyncCh),
		AsyncQueueCapacity: cap(p.asyncCh),
		LastError:          lastErr,
		WriterStats:        p.writer.Stats(),
		StartTime:          p.metrics.StartTime,
		Uptime:             time.Since(p.metrics.StartTime),
	}
}

func (p *KafkaProducer) Close() error {
	if p.closed.Swap(true) {
		return nil // already closed
	}

	p.logInfo("Closing Kafka producer...")

	// 1. Stop is already in effect (closed=true rejects new sends).
	//    Drain whatever was already accepted before we got here.
	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.Flush(flushCtx); err != nil {
		p.logErr("Timed out flushing async queue: %v", err)
	}

	// 2. Tell workers no more jobs are coming so they can drain
	//    whatever's left in the channel (should be ~empty after Flush)
	//    and exit.
	close(p.shutdownCh)
	p.workerWG.Wait()

	// 3. Close the underlying writer (flushes its own internal batch
	//    buffer for any in-progress synchronous writes).
	if err := p.writer.Close(); err != nil {
		p.logErr("Error closing Kafka writer: %v", err)
		return err
	}

	p.logInfo("Kafka producer closed successfully")
	return nil
}

// ============== INTERNAL ==============

func (p *KafkaProducer) asyncWorker() {
	defer p.workerWG.Done()

	for {
		select {
		case job, ok := <-p.asyncCh:
			if !ok {
				return
			}
			p.runJob(job)

		case <-p.shutdownCh:
			// Drain whatever's already buffered, non-blocking, then exit.
			// New sends are already rejected (closed=true) by this point,
			// so this channel only shrinks from here.
			for {
				select {
				case job, ok := <-p.asyncCh:
					if !ok {
						return
					}
					p.runJob(job)
				default:
					return
				}
			}
		}
	}
}

func (p *KafkaProducer) runJob(job asyncJob) {
	defer p.asyncWG.Done()

	ctx := job.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// Guard against a caller-supplied context with no deadline outliving
	// the worker indefinitely on a stuck broker.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.WriteTimeout)
		defer cancel()
	}

	err := p.publish(ctx, job.key, job.eventType, job.payload)
	if err != nil {
		p.metrics.recordError(err)
		p.logErr("Async produce failed for event %s: %v", job.eventType, err)
	} else {
		p.metrics.recordSuccess()
		p.logInfo("Produced event %s (key=%s) [async]", job.eventType, job.key)
	}

	if job.callback != nil {
		job.callback(err)
	}
}

func (p *KafkaProducer) publish(ctx context.Context, key, eventType string, payload []byte) error {
	topic := p.config.Topic
	if p.config.TopicRouter != nil {
		if t := p.config.TopicRouter(eventType); t != "" {
			topic = t
		}
	}
	if topic == "" {
		return fmt.Errorf("messaging: no topic resolved for event type %q", eventType)
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
		Time:  time.Now(),
	})
}

// marshalEvent wraps data in the shared Event envelope ({type, data}) so
// it's symmetric with what KafkaConsumer.processMessage expects to
// unmarshal. Avoids double-encoding if the caller already hands us
// pre-marshalled bytes.
func marshalEvent(eventType string, data interface{}) ([]byte, error) {
	var raw json.RawMessage

	switch v := data.(type) {
	case nil:
		raw = json.RawMessage("null")
	case json.RawMessage:
		raw = v
	case []byte:
		raw = json.RawMessage(v)
	default:
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		raw = b
	}

	return json.Marshal(Event{Type: eventType, Data: raw})
}

func (c *KafkaProducer) logInfo(format string, v ...interface{}) {
	if c.config.EnableLogging {
		c.logger.Printf(format, v...)
	}
}

func (c *KafkaProducer) logErr(format string, v ...interface{}) {
	if c.config.EnableLogging {
		c.errLogger.Printf(format, v...)
	}
}
