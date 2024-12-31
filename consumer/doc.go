// Package consumer provides functionality for processing published WAL (Write-Ahead Log) files
// in a concurrent and controlled manner. It implements a polling-based consumer that can
// process files with configurable concurrency and polling intervals.
//
// The consumer package is designed to work with WAL files that contain partitioned records,
// processing them through a handler interface while managing file lifecycle (reading and cleanup).
//
// Basic usage with default options:
//
//	storage := local.NewLocalStorage("pending", "published")
//	handler := &MyHandler{}
//
//	consumer := consumer.New(storage, handler, consumer.DefaultOptions())
//
//	// Start processing in background
//	ctx := context.Background()
//	go func() {
//	    if err := consumer.Start(ctx); err != nil {
//	        log.Printf("Consumer error: %v", err)
//	    }
//	}()
//
//	// Stop gracefully when done
//	consumer.Stop()
//
// Custom configuration:
//
//	opts := consumer.ConsumerOptions{
//	    PollInterval:   time.Second,
//	    MaxConcurrency: 5,
//	}
//	consumer := consumer.New(storage, handler, opts)
//
// Single processing run:
//
//	if err := consumer.Process(ctx); err != nil {
//	    log.Printf("Processing error: %v", err)
//	}
//
// The consumer requires two main interfaces to be implemented:
//
// Storage interface for managing file access:
//
//	type Storage interface {
//	    Open(ctx context.Context, path string) (ReadAtCloser, error)
//	    ListPublished(ctx context.Context) ([]string, error)
//	    Delete(ctx context.Context, path string) error
//	}
//
// Handler interface for processing records:
//
//	type Handler interface {
//	    Handle(ctx context.Context, partitionKey string, seq iter.Seq[partition.Record]) error
//	}
//
// Features:
//   - Concurrent file processing with configurable limits
//   - Automatic file cleanup after successful processing
//   - Graceful shutdown support
//   - Polling-based monitoring of new files
//   - Error handling and recovery
//   - Context cancellation support
package consumer
