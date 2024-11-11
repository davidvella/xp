package monitoring

import (
	"context"
	"time"

	"github.com/davidvella/xp/core/metrics"
)

// Stats collects and reports processing statistics
type stats struct {
	registry *metrics.Registry
	logger   *Logger
}

func NewStats(registry *metrics.Registry, logger *Logger) *stats {
	// Register default metrics
	registry.Register(metrics.Metric{
		Name:        "records_processed_total",
		Type:        metrics.Counter,
		Description: "Total number of records processed",
	})

	registry.Register(metrics.Metric{
		Name:        "windows_emitted_total",
		Type:        metrics.Counter,
		Description: "Total number of windows emitted",
	})

	registry.Register(metrics.Metric{
		Name:        "processing_latency_ms",
		Type:        metrics.Histogram,
		Description: "Processing latency in milliseconds",
	})

	registry.Register(metrics.Metric{
		Name:        "active_windows",
		Type:        metrics.Gauge,
		Description: "Number of active windows",
	})

	registry.Register(metrics.Metric{
		Name:        "active_windows",
		Type:        metrics.Gauge,
		Description: "Number of active windows",
	})

	registry.Register(metrics.Metric{
		Name:        "errors",
		Type:        metrics.Counter,
		Description: "Total number of errors by type",
	})

	return &stats{
		registry: registry,
		logger:   logger,
	}
}

func (s *stats) RecordProcessedRecord(ctx context.Context, labels map[string]string) {
	s.registry.RecordCounter("records_processed_total", 1, labels)
}

func (s *stats) RecordWindowEmitted(ctx context.Context, labels map[string]string) {
	s.registry.RecordCounter("windows_emitted_total", 1, labels)
}

func (s *stats) RecordProcessingLatency(ctx context.Context, duration time.Duration, labels map[string]string) {
	s.registry.RecordCounter("processing_latency_ms", float64(duration.Milliseconds()), labels)
}

func (s *stats) SetActiveWindows(ctx context.Context, count int, labels map[string]string) {
	s.registry.RecordGauge("active_windows", float64(count), labels)
}

func (s *stats) RecordProcessError(ctx context.Context, err string) {
	s.registry.RecordCounter("errors", 1, map[string]string{
		"error": err,
	})
}

type Stats interface {
	RecordProcessedRecord(ctx context.Context, labels map[string]string)
	RecordWindowEmitted(ctx context.Context, labels map[string]string)
	RecordProcessingLatency(ctx context.Context, duration time.Duration, labels map[string]string)
	SetActiveWindows(ctx context.Context, count int, labels map[string]string)
	RecordProcessError(ctx context.Context, err string)
}
