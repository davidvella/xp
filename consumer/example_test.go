package consumer_test

import (
	"context"
	"fmt"
	"iter"
	"log"
	"time"

	"github.com/davidvella/xp/consumer"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/storage/local"
)

// ExampleConsumer demonstrates basic usage of the consumer package.
func Example() {
	// Create storage implementation
	storage := local.NewLocalStorage("pending", "published")

	// Create handler implementation
	handler := &ExampleHandler{}

	// Create consumer with default options
	c := consumer.New(storage, handler, consumer.DefaultOptions())

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Start processing in background
	go func() {
		if err := c.Start(ctx); err != nil {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Wait for some time
	time.Sleep(500 * time.Millisecond)

	// Stop gracefully
	c.Stop()

	// Output:
}

// ExampleHandler implements the Handler interface for examples.
type ExampleHandler struct {
	processed int
}

func (h *ExampleHandler) Handle(_ context.Context, partitionKey string, seq iter.Seq[partition.Record]) error {
	for record := range seq {
		fmt.Printf("Processing record: partition=%s, id=%s\n",
			partitionKey, record.GetID())
		h.processed++
	}
	return nil
}
