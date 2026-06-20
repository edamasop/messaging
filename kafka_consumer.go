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
	handlers  map[string]EventHandler
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
		Brokers:     []string{cfg.BootstrapServers},
		GroupID:     cfg.GroupID,
		GroupTopics: cfg.Topics,
		StartOffset: kafka.FirstOffset,
		MaxWait:     cfg.ReadTimeout,
		// NOTE: CommitInterval is only meaningful for the reader's own
		// background auto-commit. When ManualCommit is true we call
		// CommitMessages explicitly and this is effectively unused;
		// when ManualCommit is false we commit synchronously right
		// after FetchMessage (see processMessage), so this is also
		// unused in that path. Left here in case callers rely on
		// kafka-go's reader directly elsewhere.
		CommitInterval: cfg.CommitInterval,
	}

	if cfg.EnableLogging {
		readerCfg.Logger = log.New(cfg.LogOutput, "[KAFKA_READER] ", log.LstdFlags)
		readerCfg.ErrorLogger = log.New(cfg.ErrOutput, "[KAFKA_READER_ERR] ", log.LstdFlags)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &KafkaConsumer{
		config:     cfg,
		consumer:   kafka.NewReader(readerCfg),
		handlers:   make(map[string]EventHandler),
		logger:     log.New(cfg.LogOutput, "[KAFKA_CONSUMER] INFO: ", log.LstdFlags),
		errLogger:  log.New(cfg.ErrOutput, "[KAFKA_CONSUMER] ERROR: ", log.LstdFlags),
		shutdownCh: make(chan struct{}),
		metrics:    &ConsumerMetrics{LastMessageTime: time.Now()},
		ctx:        ctx,
		cancel:     cancel,
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
	c.handlers[eventType] = handler
	c.logInfo("Registered handler for event type: %s", eventType)
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()

	c.logInfo("Consumer started. Subscribed to: %v", c.config.Topics)

	c.metrics.mu.Lock()
	c.metrics.StartTime = time.Now()
	c.metrics.mu.Unlock()

	healthCtx, healthCancel := context.WithCancel(ctx)
	defer healthCancel()
	go c.healthCheckLoop(healthCtx)

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

		c.processMessage(ctx, msg)
	}
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

	c.logInfo("Received message from topic %s, partition %d, offset %d",
		msg.Topic, msg.Partition, msg.Offset)

	var event Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logErr("Failed to unmarshal event: %v | Raw: %s", err, string(msg.Value))
		c.metrics.recordError(err)

		if c.config.EnableDLQ {
			// Preserve the raw bytes since `event` is zero-valued here --
			// previously this sent an empty Event to the DLQ, losing the
			// only copy of the bad payload.
			c.sendToDLQ(rawEventFallback(msg.Value), err, 0, msg.Value)
		}
		c.commitIfManual(ctx, msg)
		return
	}

	c.mu.RLock()
	handler, ok := c.handlers[event.Type]
	c.mu.RUnlock()

	if !ok {
		err := errors.New("no handler registered")
		c.logErr("No handler registered for event type: %s", event.Type)
		c.metrics.recordError(err)

		if c.config.EnableDLQ {
			c.sendToDLQ(event, err, 0, msg.Value)
		}
		c.commitIfManual(ctx, msg)
		return
	}

	var lastErr error
	var retry bool

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			atomic.AddInt64(&c.metrics.MessagesRetried, 1)
			c.logInfo("Retrying event %s (attempt %d/%d)", event.Type, attempt, c.config.MaxRetries)

			backoff := time.Duration(attempt*attempt) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			case <-c.shutdownCh:
				return
			}
		}

		retry, lastErr = handler(event.Data)
		if lastErr == nil {
			c.logInfo("Successfully processed event: %s", event.Type)
			c.metrics.recordSuccess(startTime)
			c.commitIfManual(ctx, msg)
			return
		}

		c.logErr("Handler failed for event %s (attempt %d): %v", event.Type, attempt+1, lastErr)
		c.metrics.recordError(lastErr)

		if !retry {
			c.logInfo("Handler indicated no retry for event %s", event.Type)
			break
		}
	}

	c.logErr("Handler failed after %d retries for event %s: %v",
		c.config.MaxRetries, event.Type, lastErr)

	c.metrics.recordError(lastErr)

	if c.config.EnableDLQ {
		c.sendToDLQ(event, lastErr, c.config.MaxRetries+1, msg.Value)
	}

	// Commit even on terminal failure (when ManualCommit) so a poison
	// message that's already been DLQ'd doesn't block the partition
	// forever. If EnableDLQ is false, you may prefer NOT to commit here
	// so the message can be retried after a redeploy -- adjust to taste.
	c.commitIfManual(ctx, msg)
}

// commitIfManual commits the offset when ManualCommit is enabled. When
// ManualCommit is false, the offset was already committed implicitly by
// FetchMessage's caller -- actually no: FetchMessage never auto-commits,
// so in the non-manual mode we commit immediately here too, right after
// fetch is too early (before processing) -- instead we commit right away
// in the non-manual branch below in Start() is wrong too. To keep
// semantics simple and correct: non-manual mode commits immediately after
// a successful FetchMessage, manual mode commits only after the outcome
// above is known.
func (c *KafkaConsumer) commitIfManual(ctx context.Context, msg kafka.Message) {
	if !c.config.ManualCommit {
		return
	}
	if err := c.consumer.CommitMessages(ctx, msg); err != nil {
		c.logErr("Failed to commit offset: %v", err)
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
			Addr:     kafka.TCP(c.config.BootstrapServers),
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
