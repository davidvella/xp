package xp

import (
	"time"

	"github.com/davidvella/xp/partition"
)

// options defines all configuration options for the processor.
type options struct {
	// Consumer options
	pollInterval   time.Duration // How often to poll for new files
	maxConcurrency int           // Maximum number of concurrent file processors

	// Processor options
	strategy partition.Strategy // Strategy for rotating files
}

// Option is a function that configures the processor options.
type Option func(*options)

// WithPollInterval sets the poll interval for the consumer.
func WithPollInterval(interval time.Duration) Option {
	return func(o *options) {
		o.pollInterval = interval
	}
}

// WithMaxConcurrency sets the maximum number of concurrent file processors.
func WithMaxConcurrency(m int) Option {
	return func(o *options) {
		o.maxConcurrency = m
	}
}

// WithStrategy sets the partition strategy.
func WithStrategy(strategy partition.Strategy) Option {
	return func(o *options) {
		o.strategy = strategy
	}
}

// defaultOptions returns the default configuration.
func defaultOptions() options {
	return options{
		pollInterval:   5 * time.Second,
		maxConcurrency: 10,
		strategy:       nil,
	}
}
