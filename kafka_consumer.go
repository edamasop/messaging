package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// ============== CONFIGURATION ==============

type ConsumerConfig struct {
	BootstrapServers string
	GroupID          string
	Topics           []string
	EnableLogging    bool
	LogOutput        io.Writer
	ErrOutput        io.Writer

	// Read timeout per message
	ReadTimeout time.Duration
	// Backoff between errors
	ErrorBackoff time.Duration
	// Maximum retry attempts per message
	MaxRetries int
	// Auto-commit interval (only relevant when ManualCommit == false)
	CommitInterval time.Duration
	// Manual commit mode (if true, commits only after the handler
	// succeeds, or after retries+DLQ are exhausted on failure).
	// If false, the offset is committed immediately after the message
	// is fetched (at-most-once-ish, but simple).
	ManualCommit bool
	// StartOffset selects where a consumer group with no committed offsets
	// begins: kafka.FirstOffset replays the whole topic, kafka.LastOffset
	// only sees messages published from now on. Defaults to FirstOffset so
	// a fresh local setup still replays history.
	StartOffset int64
	// ProcessingWorkers is the number of partition workers. Every partition
	// is pinned to exactly one worker, so per-partition ordering is kept
	// while different partitions are handled concurrently. A handler that
	// is slow on one partition therefore no longer stalls the others.
	// Defaults to 4, capped at 64.
	ProcessingWorkers int
	// PartitionBufferSize bounds each worker's queue. When a queue fills up,
	// fetching blocks: that is deliberate backpressure towards Kafka instead
	// of unbounded in-memory growth. Defaults to 64.
	PartitionBufferSize int
	// Dead Letter Queue configuration
	EnableDLQ  bool
	DLQTopic   string
	DLQHandler DeadLetterHandler
	// Health check configuration
	HealthCheckInterval time.Duration
	// Number of failures within HealthFailureWindow that flips
	// IsHealthy to false. Defaults to 10 if <= 0.
	UnhealthyFailureThreshold int
	// Sliding window used to evaluate UnhealthyFailureThreshold.
	// Defaults to 1 minute if <= 0.
	HealthFailureWindow time.Duration
}

// ============== MAIN CONSUMER ==============

type KafkaConsumer struct {
	config    ConsumerConfig
	consumer  *kafka.Reader
	handlers  map[string][]EventHandler
	logger    *log.Logger
	errLogger *log.Logger

	// Shutdown coordination
	wg         sync.WaitGroup
	shutdownCh chan struct{}
	closed     atomic.Bool
	mu         sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc

	// Metrics
	metrics *ConsumerMetrics

	// DLQ producer (lazy initialized)
	dlqWriter *kafka.Writer
	dlqMu     sync.Mutex

	// Pause/resume state. Both `paused` and `resumeChan` are guarded by
	// pauseMu -- previously `resumeChan` was reassigned by Pause()/Resume()
	// while Start()'s loop read it unsynchronized, which is a data race
	// (the channel *pointer*, not just isPaused, needs protection).
	pauseMu    sync.Mutex
	paused     bool
	resumeChan chan struct{}

	// workCh holds one queue per partition worker. The fetch loop in Start()
	// is the only writer (and the only closer), workers are the readers, so
	// no extra locking is needed for the slice itself.
	workCh   []chan kafka.Message
	workerWG sync.WaitGroup
}

// ============== CONSTRUCTOR ==============

func NewKafkaConsumer(cfg ConsumerConfig) (*KafkaConsumer, error) {
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stderr
	}
	if cfg.ErrOutput == nil {
		cfg.ErrOutput = os.Stderr
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.ErrorBackoff <= 0 {
		cfg.ErrorBackoff = 2 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.CommitInterval <= 0 {
		cfg.CommitInterval = 5 * time.Second
	}
	if cfg.HealthCheckInterval <= 0 {
		cfg.HealthCheckInterval = 30 * time.Second
	}
	if cfg.UnhealthyFailureThreshold <= 0 {
		cfg.UnhealthyFailureThreshold = 10
	}
	if cfg.HealthFailureWindow <= 0 {
		cfg.HealthFailureWindow = time.Minute
	}
	if cfg.StartOffset == 0 {
		cfg.StartOffset = kafka.FirstOffset
	}
	if cfg.ProcessingWorkers <= 0 {
		cfg.ProcessingWorkers = 4
	}
	if cfg.ProcessingWorkers > 64 {
		cfg.ProcessingWorkers = 64
	}
	if cfg.PartitionBufferSize <= 0 {
		cfg.PartitionBufferSize = 64
	}

	if cfg.EnableDLQ && cfg.DLQTopic == "" {
		return nil, errors.New("DLQ topic must be specified when EnableDLQ is true")
	}
	if cfg.BootstrapServers == "" {
		return nil, errors.New("BootstrapServers must be specified")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("GroupID must be specified")
	}
	if len(cfg.Topics) == 0 {
		return nil, errors.New("at least one topic must be specified")
	}

	readerCfg := kafka.ReaderConfig{
		// splitServers is required here: BootstrapServers is a comma-separated
		// list, and handing "a:9092,b:9092" to kafka-go as a single address
		// would make the consumer dial a host that does not exist.
		Brokers:     splitServers(cfg.BootstrapServers),
		GroupID:     cfg.GroupID,
		GroupTopics: cfg.Topics,
		StartOffset: cfg.StartOffset,
		MaxWait:     cfg.ReadTimeout,
		// NOTE: CommitInterval is only meaningful for the reader's own
		// background auto-commit. When ManualCommit is true we call
		// CommitMessages explicitly and this is effectively unused;
		// when ManualCommit is false Start() commits synchronously right
		// after FetchMessage, so it is unused there as well.
		CommitInterval: cfg.CommitInterval,
	}

	if cfg.EnableLogging {
		readerCfg.Logger = log.New(cfg.LogOutput, "[KAFKA_READER] ", log.LstdFlags)
		readerCfg.ErrorLogger = log.New(cfg.ErrOutput, "[KAFKA_READER_ERR] ", log.LstdFlags)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// One queue per partition worker. Buffered so a burst of already-fetched
	// messages does not immediately block the fetch loop.
	workCh := make([]chan kafka.Message, cfg.ProcessingWorkers)
	for i := range workCh {
		workCh[i] = make(chan kafka.Message, cfg.PartitionBufferSize)
	}

	return &KafkaConsumer{
		config:     cfg,
		consumer:   kafka.NewReader(readerCfg),
		handlers:   make(map[string][]EventHandler),
		logger:     log.New(cfg.LogOutput, "[KAFKA_CONSUMER] INFO: ", log.LstdFlags),
		errLogger:  log.New(cfg.ErrOutput, "[KAFKA_CONSUMER] ERROR: ", log.LstdFlags),
		shutdownCh: make(chan struct{}),
		metrics:    &ConsumerMetrics{LastMessageTime: time.Now()},
		ctx:        ctx,
		cancel:     cancel,
		workCh:     workCh,
		// Initialize resumeChan up front (closed channels are valid to
		// select on repeatedly only once; an *unstarted* paused state
		// never happens here since paused starts false, so this is just
		// for safety / to avoid a nil-channel edge case).
		resumeChan: make(chan struct{}),
	}, nil
}

// ============== PUBLIC METHODS ==============

func (c *KafkaConsumer) RegisterHandler(eventType string, handler EventHandler) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.handlers[eventType] = append(c.handlers[eventType], handler)

	c.logInfo(
		"Registered handler #%d for event type: %s",
		len(c.handlers[eventType]),
		eventType,
	)
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()

	c.logInfo(
		"Consumer started. Subscribed to: %v, partition workers: %d",
		c.config.Topics,
		len(c.workCh),
	)

	c.metrics.mu.Lock()
	c.metrics.StartTime = time.Now()
	c.metrics.mu.Unlock()

	healthCtx, healthCancel := context.WithCancel(ctx)
	defer healthCancel()
	go c.healthCheckLoop(healthCtx)

	for i := range c.workCh {
		c.workerWG.Add(1)
		go c.partitionWorker(i, c.workCh[i])
	}

	// Start() is the only producer on the work channels and therefore the
	// only goroutine allowed to close them: no send/close race is possible.
	// Draining before returning means a graceful shutdown still commits the
	// messages that were already fetched.
	defer func() {
		for _, ch := range c.workCh {
			close(ch)
		}
		c.workerWG.Wait()
		c.logInfo("Partition workers stopped")
	}()

	for {
		// Snapshot pause state + the channel to wait on under the lock,
		// so we never read a resumeChan that's being concurrently
		// replaced by Pause().
		c.pauseMu.Lock()
		paused := c.paused
		waitCh := c.resumeChan
		c.pauseMu.Unlock()

		if paused {
			c.logInfo("Consumption is currently paused. Waiting for resume signal...")
			select {
			case <-waitCh:
				c.logInfo("Resume signal received. Continuing processing...")
			case <-ctx.Done():
				c.logInfo("Context cancelled while paused, stopping consumer...")
				return
			case <-c.shutdownCh:
				c.logInfo("Shutdown signal received while paused, stopping consumer...")
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			c.logInfo("Context cancelled, stopping consumer...")
			return
		case <-c.shutdownCh:
			c.logInfo("Shutdown signal received, stopping consumer...")
			return
		default:
		}

		// FetchMessage (unlike ReadMessage) does NOT auto-commit the
		// offset. This is what makes ManualCommit actually mean
		// something: the offset only advances once we decide to commit
		// it, after the handler has run (or after DLQ).
		msg, err := c.consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			c.logErr("Message read error: %v. Retrying in %v...", err, c.config.ErrorBackoff)
			c.metrics.recordError(err)

			select {
			case <-time.After(c.config.ErrorBackoff):
				continue
			case <-ctx.Done():
				return
			case <-c.shutdownCh:
				return
			}
		}

		// Without manual commit we deliberately keep the old "at most once"
		// contract: the offset advances as soon as the message is fetched,
		// not when it has been handled.
		if !c.config.ManualCommit {
			c.commitMessage(msg)
		}

		if err := c.dispatch(ctx, msg); err != nil {
			c.logInfo("Stopped dispatching messages: %v", err)
			return
		}
	}
}

// dispatch hands a message to the worker that owns its partition. It blocks
// while that worker's queue is full, which is the intended backpressure: we
// stop fetching instead of buffering an unbounded amount of work in memory.
func (c *KafkaConsumer) dispatch(ctx context.Context, msg kafka.Message) error {
	idx := partitionWorkerIndex(msg.Partition, len(c.workCh))

	select {
	case c.workCh[idx] <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.shutdownCh:
		return ErrConsumerClosed
	}
}

// partitionWorker processes messages of its partition strictly one by one,
// which preserves per-partition ordering (Kafka only guarantees ordering
// inside a partition, so no stronger guarantee is possible or needed).
func (c *KafkaConsumer) partitionWorker(id int, ch <-chan kafka.Message) {
	defer c.workerWG.Done()

	c.logInfo("Partition worker %d started", id)

	for msg := range ch {
		// The worker context is intentionally not the Start() context: on a
		// graceful stop we want to finish what was already fetched. A hard
		// shutdown still aborts retries through shutdownCh.
		c.processMessage(context.Background(), msg)
	}

	c.logInfo("Partition worker %d stopped", id)
}

// partitionWorkerIndex pins a partition to a worker. The modulo mapping is
// what keeps ordering intact: the same partition always lands on the same
// worker and is therefore never processed by two goroutines at once.
func partitionWorkerIndex(partition int, workers int) int {
	if workers <= 1 || partition < 0 {
		return 0
	}
	return partition % workers
}

func (c *KafkaConsumer) Close() error {
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.logInfo("Closing Kafka consumer...")

	close(c.shutdownCh)
	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logInfo("All messages processed gracefully")
	case <-time.After(30 * time.Second):
		c.logErr("Shutdown timeout - forcing close")
	}

	if c.dlqWriter != nil {
		if err := c.dlqWriter.Close(); err != nil {
			c.logErr("Error closing DLQ writer: %v", err)
		}
	}

	if err := c.consumer.Close(); err != nil {
		c.logErr("Error closing Kafka consumer: %v", err)
		return err
	}

	c.logInfo("Kafka consumer closed successfully")
	return nil
}

// Pause halts processing before pulling the next message.
func (c *KafkaConsumer) Pause() error {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if !c.paused {
		c.paused = true
		c.resumeChan = make(chan struct{})
		c.logInfo("Consumer consumption paused.")
	}
	return nil
}

// Resume allows the consumption loop to continue.
func (c *KafkaConsumer) Resume() error {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.paused {
		c.paused = false
		close(c.resumeChan)
		c.logInfo("Consumer consumption resumed.")
	}
	return nil
}

// GetHealth returns the current health status.
func (c *KafkaConsumer) GetHealth() HealthStatus {
	stats := c.consumer.Stats()

	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()

	return HealthStatus{
		IsHealthy:         c.metrics.recentFailureCountLocked(c.config.HealthFailureWindow) < c.config.UnhealthyFailureThreshold,
		IsRunning:         !c.closed.Load(),
		MessagesProcessed: atomic.LoadInt64(&c.metrics.MessagesProcessed),
		MessagesFailed:    atomic.LoadInt64(&c.metrics.MessagesFailed),
		MessagesRetried:   atomic.LoadInt64(&c.metrics.MessagesRetried),
		MessagesDLQ:       atomic.LoadInt64(&c.metrics.MessagesDLQ),
		LastMessageTime:   c.metrics.LastMessageTime,
		LastError:         c.getLastErrorStringLocked(),
		ConsumerStats:     stats,
		StartTime:         c.metrics.StartTime,
		Uptime:            time.Since(c.metrics.StartTime),
	}
}

// GetMetrics returns a snapshot of current metrics.
func (c *KafkaConsumer) GetMetrics() ConsumerMetrics {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()
	// Copy without the mutex to avoid copying a locked sync.RWMutex by value.
	return ConsumerMetrics{
		StartTime:          c.metrics.StartTime,
		MessagesProcessed:  c.metrics.MessagesProcessed,
		MessagesFailed:     c.metrics.MessagesFailed,
		MessagesRetried:    c.metrics.MessagesRetried,
		MessagesDLQ:        c.metrics.MessagesDLQ,
		LastMessageTime:    c.metrics.LastMessageTime,
		LastError:          c.metrics.LastError,
		ProcessingDuration: c.metrics.ProcessingDuration,
	}
}

// ============== INTERNAL PROCESSING ==============

func (c *KafkaConsumer) processMessage(ctx context.Context, msg kafka.Message) {
	startTime := time.Now()

	c.logInfo(
		"Received message from topic %s, partition %d, offset %d",
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)

	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logErr("Failed to unmarshal event: %v | Raw: %s", err, string(msg.Value))
		c.metrics.recordError(err)

		if c.config.EnableDLQ {
			c.sendToDLQ(rawEventFallback(msg.Value), err, 0, msg.Value)
		}

		c.commitAfterProcessing(msg)
		return
	}

	c.mu.RLock()
	handlers := append([]EventHandler(nil), c.handlers[event.Type]...)
	c.mu.RUnlock()

	if len(handlers) == 0 {
		c.logInfo("Skipping event %s: no handler registered", event.Type)
		c.metrics.recordSuccess(startTime)
		c.commitAfterProcessing(msg)
		return
	}

	for _, handler := range handlers {
		if err := c.executeHandler(ctx, event, handler); err != nil {
			// A shutdown in the middle of a retry is not a failure of the
			// event: leave the offset uncommitted so the message is redelivered
			// to the next instance instead of being parked in the DLQ.
			if errors.Is(err, errConsumerStopping) {
				c.logInfo(
					"Consumer stopping; leaving event %s (topic=%s partition=%d offset=%d) uncommitted for redelivery",
					event.Type, msg.Topic, msg.Partition, msg.Offset,
				)
				return
			}

			c.metrics.recordError(err)

			if c.config.EnableDLQ {
				c.sendToDLQ(event, err, c.config.MaxRetries+1, msg.Value)
			}

			c.commitAfterProcessing(msg)
			return
		}
	}

	c.logInfo(
		"Successfully processed event %s with %d handler(s)",
		event.Type,
		len(handlers),
	)

	c.metrics.recordSuccess(startTime)
	c.commitAfterProcessing(msg)
}

func (c *KafkaConsumer) executeHandler(
	ctx context.Context,
	event Event,
	handler EventHandler,
) error {

	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {

		if attempt > 0 {
			atomic.AddInt64(&c.metrics.MessagesRetried, 1)

			c.logInfo(
				"Retrying event %s (attempt %d/%d)",
				event.Type,
				attempt,
				c.config.MaxRetries,
			)

			backoff := time.Duration(attempt*attempt) * time.Second

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return errConsumerStopping
			case <-c.shutdownCh:
				return errConsumerStopping
			}
		}

		retry, err := handler(event.Data)
		if err == nil {
			return nil
		}

		lastErr = err

		c.logErr(
			"Handler failed for event %s (attempt %d): %v",
			event.Type,
			attempt+1,
			err,
		)

		if !retry {
			break
		}
	}

	return lastErr
}

// commitAfterProcessing commits the offset once the handler outcome is known.
// It is a no-op without manual commit, where the offset has already been
// committed right after the fetch (see Start).
func (c *KafkaConsumer) commitAfterProcessing(msg kafka.Message) {
	if !c.config.ManualCommit {
		return
	}

	c.commitMessage(msg)
}

// commitMessage commits a single offset. A dedicated timeout is used instead
// of the caller's context so that an in-flight message can still be committed
// while the service is shutting down.
func (c *KafkaConsumer) commitMessage(msg kafka.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.consumer.CommitMessages(ctx, msg); err != nil {
		c.logErr("Failed to commit offset (topic=%s partition=%d offset=%d): %v",
			msg.Topic, msg.Partition, msg.Offset, err)
		c.metrics.recordError(err)
	}
}

// ============== DEAD LETTER QUEUE ==============

func (c *KafkaConsumer) sendToDLQ(event Event, err error, attempts int, raw []byte) {
	atomic.AddInt64(&c.metrics.MessagesDLQ, 1)
	c.logErr("Sending event %s to DLQ (attempts: %d, error: %v)", event.Type, attempts, err)

	if c.config.DLQHandler != nil {
		if hErr := c.config.DLQHandler(event, err, attempts); hErr != nil {
			c.logErr("DLQ handler failed: %v", hErr)
		}
		return
	}

	c.dlqMu.Lock()
	defer c.dlqMu.Unlock()

	if c.dlqWriter == nil {
		c.dlqWriter = &kafka.Writer{
			Addr:     kafka.TCP(splitServers(c.config.BootstrapServers)...),
			Topic:    c.config.DLQTopic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	dlqMsg := DLQMessage{
		OriginalEvent: event,
		RawMessage:    raw,
		Error:         err.Error(),
		Attempts:      attempts,
		Timestamp:     time.Now(),
		ConsumerGroup: c.config.GroupID,
	}

	msgBytes, mErr := json.Marshal(dlqMsg)
	if mErr != nil {
		c.logErr("Failed to marshal DLQ message: %v", mErr)
		return
	}

	dlqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if wErr := c.dlqWriter.WriteMessages(dlqCtx, kafka.Message{
		Key:   []byte(event.Type),
		Value: msgBytes,
	}); wErr != nil {
		c.logErr("Failed to send to DLQ: %v", wErr)
	}
}

// rawEventFallback builds a best-effort Event when JSON unmarshalling
// itself failed, so downstream DLQ consumers at least see something
// instead of a zero-value struct. Adjust field names to match your real
// Event type.
func rawEventFallback(raw []byte) Event {
	return Event{
		Type: "UNPARSEABLE",
		Data: json.RawMessage(raw),
	}
}

// ============== HEALTH CHECK ==============

func (c *KafkaConsumer) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(c.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			health := c.GetHealth()
			if !health.IsHealthy {
				c.logErr("Health check failed: %+v", health)
			} else {
				c.logInfo("Health check passed: processed=%d, failed=%d",
					health.MessagesProcessed, health.MessagesFailed)
			}
		}
	}
}

func (c *KafkaConsumer) logInfo(format string, v ...interface{}) {
	if c.config.EnableLogging {
		c.logger.Printf(format, v...)
	}
}

func (c *KafkaConsumer) logErr(format string, v ...interface{}) {
	if c.config.EnableLogging {
		c.errLogger.Printf(format, v...)
	}
}

// ============== METRICS IMPLEMENTATION ==============

func (m *ConsumerMetrics) recordSuccess(startTime time.Time) {
	atomic.AddInt64(&m.MessagesProcessed, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastMessageTime = time.Now()

	duration := time.Since(startTime)
	m.totalProcessed++
	m.totalDuration += duration
	m.ProcessingDuration = m.totalDuration / time.Duration(m.totalProcessed)
}

func (m *ConsumerMetrics) recordError(err error) {
	atomic.AddInt64(&m.MessagesFailed, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastError = err
	now := time.Now()
	m.recentFailures = append(m.recentFailures, now)
	// Trim anything older than a generous bound (5 min) so this slice
	// can't grow unbounded on a long-running consumer with a high error
	// rate. recentFailureCountLocked does the precise window filtering.
	cutoff := now.Add(-5 * time.Minute)
	i := 0
	for i < len(m.recentFailures) && m.recentFailures[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		m.recentFailures = m.recentFailures[i:]
	}
}

// recentFailureCountLocked returns the number of failures within the
// given window. Caller must hold m.mu (R or W lock).
func (m *ConsumerMetrics) recentFailureCountLocked(window time.Duration) int {
	cutoff := time.Now().Add(-window)
	count := 0
	for i := len(m.recentFailures) - 1; i >= 0; i-- {
		if m.recentFailures[i].Before(cutoff) {
			break
		}
		count++
	}
	return count
}

func (c *KafkaConsumer) getLastErrorStringLocked() string {
	if c.metrics.LastError != nil {
		return c.metrics.LastError.Error()
	}
	return ""
}

// DLQMessage represents a message sent to the dead letter queue.
type DLQMessage struct {
	OriginalEvent Event           `json:"original_event"`
	RawMessage    json.RawMessage `json:"raw_message,omitempty"`
	Error         string          `json:"error"`
	Attempts      int             `json:"attempts"`
	Timestamp     time.Time       `json:"timestamp"`
	ConsumerGroup string          `json:"consumer_group"`
}
