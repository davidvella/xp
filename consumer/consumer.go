package consumer

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/davidvella/xp"
	"github.com/davidvella/xp/processor"
	"github.com/davidvella/xp/wal"
)

// Storage defines the interface for reading published files.
type Storage interface {
	// Open a published file/object for reading.
	Open(ctx context.Context, path string) (ReadAtCloser, error)
	// ListPublished List published files/objects.
	ListPublished(ctx context.Context) ([]string, error)
	// Delete a published file/object after processing.
	Delete(ctx context.Context, path string) error
}

type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}

type Consumer struct {
	storage         Storage
	handler         xp.Handler
	pollInterval    time.Duration
	maxConcurrency  int
	processingFiles sync.Map
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

type Options struct {
	PollInterval   time.Duration
	MaxConcurrency int
}

// DefaultOptions returns the default configuration options.
func DefaultOptions() Options {
	return Options{
		PollInterval:   5 * time.Second,
		MaxConcurrency: 10,
	}
}

func New(storage Storage, handler xp.Handler, opts Options) *Consumer {
	return &Consumer{
		storage:        storage,
		handler:        handler,
		pollInterval:   opts.PollInterval,
		maxConcurrency: opts.MaxConcurrency,
		stopChan:       make(chan struct{}),
	}
}

// Start begins the background polling and processing of files.
func (c *Consumer) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	// Process immediately on start
	if err := c.poll(ctx); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopChan:
			return nil
		case <-ticker.C:
			if err := c.poll(ctx); err != nil {
				// Log error but continue polling
				fmt.Printf("Error polling: %v\n", err)
			}
		}
	}
}

// Stop gracefully shuts down the consumer.
func (c *Consumer) Stop() {
	close(c.stopChan)
	c.wg.Wait()
}

func (c *Consumer) poll(ctx context.Context) error {
	files, err := c.storage.ListPublished(ctx)
	if err != nil {
		return fmt.Errorf("failed to list published files: %w", err)
	}

	// Create a semaphore channel to limit concurrent processing
	sem := make(chan struct{}, c.maxConcurrency)

	for _, file := range files {
		// Skip if file is already being processed
		if _, exists := c.processingFiles.LoadOrStore(file, struct{}{}); exists {
			continue
		}

		// Wait for space in the semaphore
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		c.wg.Add(1)
		go func(file string) {
			defer func() {
				c.processingFiles.Delete(file)
				<-sem
				c.wg.Done()
			}()

			if err := c.processFile(ctx, file); err != nil {
				fmt.Printf("Error processing file %s: %v\n", file, err)
			}
		}(file)
	}

	return nil
}

func (c *Consumer) processFile(ctx context.Context, path string) error {
	wk, err := processor.Deserialize(path)
	if err != nil {
		return fmt.Errorf("consumer: not valid wal file: %w", err)
	}

	reader, err := c.storage.Open(ctx, path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer reader.Close()

	r := wal.NewReader(reader)

	if err := c.handler.Handle(ctx, wk.PartitionKey, r.All()); err != nil {
		return err
	}

	if err := c.storage.Delete(ctx, path); err != nil {
		return err
	}

	return nil
}

// Process reads and processes all published files once.
func (c *Consumer) Process(ctx context.Context) error {
	files, err := c.storage.ListPublished(ctx)
	if err != nil {
		return fmt.Errorf("failed to list published files: %w", err)
	}

	for _, file := range files {
		if err := c.processFile(ctx, file); err != nil {
			return fmt.Errorf("failed to process file %s: %w", file, err)
		}
	}

	return nil
}
