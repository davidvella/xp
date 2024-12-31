package xp

import (
	"context"
	"errors"
	"sync"

	"github.com/davidvella/xp/consumer"
	"github.com/davidvella/xp/handler"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/processor"
)

// Processor manages both writing and consuming of records.
type Processor struct {
	producer    *processor.Processor
	consumer    *consumer.Consumer
	shutdownCh  chan struct{}
	shutdownErr error
	ctx         context.Context
	cancel      context.CancelFunc
	once        sync.Once
}

// NewProcessor creates a new processor instance with the given storage and handler.
func NewProcessor(
	producerStorage processor.Storage,
	consumerStorage consumer.Storage,
	handler handler.Handler,
	opts ...Option,
) (*Processor, error) {
	// Apply default options
	o := defaultOptions()

	// Apply user options
	for _, opt := range opts {
		opt(&o)
	}

	if o.strategy == nil {
		return nil, errors.New("xp: partition strategy is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create the producer processor
	p := processor.New(producerStorage, o.strategy)

	// Create the consumer
	c := consumer.New(
		consumerStorage,
		handler,
		consumer.Options{
			PollInterval:   o.pollInterval,
			MaxConcurrency: o.maxConcurrency,
		},
	)

	proc := &Processor{
		producer:   p,
		consumer:   c,
		shutdownCh: make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start the consumer immediately
	go func() {
		if err := c.Start(ctx); !errors.Is(err, context.Canceled) {
			proc.shutdownErr = err
		}
		close(proc.shutdownCh)
	}()

	return proc, nil
}

// Handle processes a single record through the producer.
func (p *Processor) Handle(ctx context.Context, record partition.Record) error {
	p.once.Do(func() {
		err := p.producer.Recover(ctx)
		if err != nil {
			panic(err)
		}
	})
	return p.producer.Handle(ctx, record)
}

// Stop gracefully shuts down both producer and consumer.
func (p *Processor) Stop() error {
	// Cancel the context
	p.cancel()

	// Stop the consumer
	p.consumer.Stop()

	// Wait for shutdown to complete
	<-p.shutdownCh

	// Close the producer
	if err := p.producer.Close(p.ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			return err
		}
	}

	return p.shutdownErr
}
