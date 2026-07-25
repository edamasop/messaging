package messaging

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer publishes events to Kafka.
//
// This is intentionally broader than a single blocking Produce() call:
// real services need both "block until acked" (e.g. before returning 200
// from an HTTP handler) and "fire and forget but tell me if it failed"
// (e.g. emitting analytics events from a hot path) -- plus a way to
// guarantee the latter actually landed before shutdown.
type Producer interface {
	// Produce publishes an event synchronously
	// and blocks until the broker acks or ctx is done.
	Produce(ctx context.Context, eventType string, data interface{}) error

	// ProduceKey is like Produce but with an explicit partition key.
	// Use this whenever you need ordering/co-location guarantees across
	// different event types for the same logical entity (e.g. key by
	// order ID rather than event type).
	ProduceKey(ctx context.Context, key string, eventType string, data interface{}) error

	// ProduceAsync enqueues an event without blocking for the broker ack.
	// callback (may be nil) is invoked exactly once, from a worker
	// goroutine, with the result. If the internal queue is full,
	// ProduceAsync blocks until space frees up or ctx is done -- it
	// never silently drops messages or grows memory unboundedly.
	ProduceAsync(ctx context.Context, key string, eventType string, data interface{}, callback func(error))

	// Flush blocks until every message accepted by ProduceAsync so far
	// has been sent (successfully or not), or until ctx is done.
	Flush(ctx context.Context) error

	// Health returns current producer health and metrics.
	Health() ProducerHealth

	// Close flushes any pending async messages, stops accepting new
	// sends, and releases the underlying connection. Safe to call once;
	// subsequent calls are no-ops returning nil.
	Close() error
}

// ErrProducerClosed is returned (or passed to async callbacks) once Close
// has been called and no further sends are accepted.
var ErrProducerClosed = errors.New("messaging: producer is closed")

// ProducerHealth mirrors the consumer's HealthStatus shape so both sides
// of a service can be monitored the same way.
type ProducerHealth struct {
	IsHealthy          bool              `json:"is_healthy"`
	MessagesSent       int64             `json:"messages_sent"`
	MessagesFailed     int64             `json:"messages_failed"`
	AsyncQueueLength   int               `json:"async_queue_length"`
	AsyncQueueCapacity int               `json:"async_queue_capacity"`
	LastError          string            `json:"last_error,omitempty"`
	WriterStats        kafka.WriterStats `json:"writer_stats"`
	StartTime          time.Time         `json:"start_time"`
	Uptime             time.Duration     `json:"uptime"`
}

// ProducerMetrics tracks counters plus a small sliding window of recent
// failures, used for a rate-based (not lifetime-cumulative) health signal.
type ProducerMetrics struct {
	mu             sync.RWMutex
	StartTime      time.Time
	MessagesSent   int64
	MessagesFailed int64
	LastError      error
	recentFailures []time.Time
}

func (m *ProducerMetrics) recordSuccess() {
	atomic.AddInt64(&m.MessagesSent, 1)
}

func (m *ProducerMetrics) recordError(err error) {
	atomic.AddInt64(&m.MessagesFailed, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.LastError = err
	now := time.Now()
	m.recentFailures = append(m.recentFailures, now)
	cutoff := now.Add(-5 * time.Minute)
	i := 0
	for i < len(m.recentFailures) && m.recentFailures[i].Before(cutoff) {
		i++
	}
	if i > 0 {
		m.recentFailures = m.recentFailures[i:]
	}
}

func (m *ProducerMetrics) recentFailureCountLocked(window time.Duration) int {
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
