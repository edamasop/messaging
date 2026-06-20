package messaging

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Consumer defines the behavioral contract for an asynchronous message consumer.
// It supports event-driven registration, lifecycle control (start, close, pause, resume),
// and deep observability through health and metric reporting.
//
// Implementations must ensure thread safety across all methods, as they are
// designed to be controlled and monitored from separate goroutines.
type Consumer interface {
	// Start begins pulling and processing messages from the underlying messaging system.
	// This method blocks until the provided context is canceled, the consumer is explicitly
	// closed via Close(), or a fatal unrecoverable error occurs.
	Start(ctx context.Context)

	// Close gracefully shuts down the consumer. It stops fetching new messages,
	// flushes pending operations (like dead-letter queues or offset commits),
	// waits for current active message handlers to finish execution, and releases
	// all internal network resources.
	Close() error

	// RegisterHandler maps a specific event type to an EventHandler.
	// When a message matching the eventType is received, the registered handler
	// will be invoked to process the payload. Registering multiple handlers for
	// the same event type typically overwrites the previous registration.
	RegisterHandler(eventType string, handler EventHandler)

	// Pause temporarily halts the consumption loop, preventing the consumer
	// from pulling new messages. Active handlers already running in the background
	// are permitted to finish processing.
	Pause() error

	// Resume restarts message consumption after it has been halted by Pause().
	// If the consumer is not currently paused, this operation is a no-op.
	Resume() error

	// GetHealth evaluates the current operating status of the consumer and returns
	// a point-in-time health report. This is typically used by external monitoring
	// systems or liveness probes to verify if the consumer loop is stalling or failing.
	GetHealth() HealthStatus

	// GetMetrics retrieves a snapshot of the performance and operational metrics
	// of the consumer (e.g., counters for messages processed, failed, retried, or
	// routed to a Dead Letter Queue).
	GetMetrics() ConsumerMetrics
}

// EventHandler returns (shouldRetry bool, error)
type EventHandler func(json.RawMessage) (bool, error)

// DeadLetterHandler handles messages that failed all retries
type DeadLetterHandler func(event Event, originalError error, attempts int) error

// HealthStatus represents the current health of the consumer
type HealthStatus struct {
	IsHealthy         bool              `json:"is_healthy"`
	IsRunning         bool              `json:"is_running"`
	MessagesProcessed int64             `json:"messages_processed"`
	MessagesFailed    int64             `json:"messages_failed"`
	MessagesRetried   int64             `json:"messages_retried"`
	MessagesDLQ       int64             `json:"messages_dlq"`
	LastMessageTime   time.Time         `json:"last_message_time"`
	LastError         string            `json:"last_error,omitempty"`
	ConsumerStats     kafka.ReaderStats `json:"consumer_stats"`
	StartTime         time.Time         `json:"start_time"`
	Uptime            time.Duration     `json:"uptime"`
}

// ConsumerMetrics provides monitoring data.
// All fields are guarded by mu; the int64 counters are ALSO updated via
// atomic ops so GetHealth() can read them without taking the lock from
// multiple goroutines. (Belt-and-suspenders: atomic for hot-path counters,
// mutex for the non-atomic fields like LastError/durations.)
type ConsumerMetrics struct {
	mu                 sync.RWMutex
	StartTime          time.Time
	MessagesProcessed  int64
	MessagesFailed     int64
	MessagesRetried    int64
	MessagesDLQ        int64
	LastMessageTime    time.Time
	LastError          error
	ProcessingDuration time.Duration // rolling average processing time
	totalDuration      time.Duration
	totalProcessed     int64

	// recentFailures is a tiny ring buffer of failure timestamps used to
	// compute a *rate-based* health signal instead of a lifetime counter,
	// so a consumer that had a bad five minutes a week ago doesn't stay
	// "unhealthy" forever.
	recentFailures []time.Time
}
