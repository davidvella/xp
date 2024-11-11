package metrics

import (
	"sync"
	"time"
)

// MetricType represents different types of metrics
type MetricType int

const (
	Counter MetricType = iota
	Gauge
	Histogram
)

// Metric represents a single metric
type Metric struct {
	Name        string
	Type        MetricType
	Description string
	Labels      map[string]string
}

// MetricValue represents the value of a metric
type MetricValue struct {
	Value     float64
	Timestamp time.Time
	Labels    map[string]string
}

// Registry stores and manages metrics
type Registry struct {
	metrics map[string]Metric
	values  map[string][]MetricValue
	mu      sync.RWMutex
}

func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]Metric),
		values:  make(map[string][]MetricValue),
	}
}

func (r *Registry) Register(metric Metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics[metric.Name] = metric
}

func (r *Registry) RecordCounter(name string, value float64, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.metrics[name]; ok && metric.Type == Counter {
		r.values[name] = append(r.values[name], MetricValue{
			Value:     value,
			Timestamp: time.Now(),
			Labels:    labels,
		})
	}
}

func (r *Registry) RecordGauge(name string, value float64, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if metric, ok := r.metrics[name]; ok && metric.Type == Gauge {
		r.values[name] = []MetricValue{{
			Value:     value,
			Timestamp: time.Now(),
			Labels:    labels,
		}}
	}
}

func (r *Registry) GetMetrics() map[string][]MetricValue {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make(map[string][]MetricValue)
	for name, values := range r.values {
		result[name] = append([]MetricValue{}, values...)
	}
	return result
}
