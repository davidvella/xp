package xp_test

import (
	"context"
	"fmt"
	"iter"
	"os"
	"time"

	"github.com/davidvella/xp"
	"github.com/davidvella/xp/handler"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/partition/strategy/messagecount"
	"github.com/davidvella/xp/storage/local"
)

// ExampleProcessor demonstrates how to use the Processor type.
func ExampleProcessor() {
	pendingDir, err := os.MkdirTemp("", "pending-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	publishedDir, err := os.MkdirTemp("", "published-*")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	// Create storages
	storage := local.NewLocalStorage(pendingDir, publishedDir)

	// Create a handler that processes records
	h := handler.Func(func(_ context.Context, _ string, seq iter.Seq[partition.Record]) error {
		for rec := range seq {
			fmt.Printf("Processing record: %s\n", rec.GetID())
		}
		return nil
	})

	// Create a new processor with round-robin strategy
	proc, err := xp.NewProcessor(
		storage,
		storage,
		h,
		xp.WithStrategy(messagecount.NewStrategy(2)),
		xp.WithMaxConcurrency(4), // Process up to 4 records concurrently
		xp.WithPollInterval(250*time.Millisecond),
	)
	if err != nil {
		fmt.Printf("Failed to create processor: %v\n", err)
		return
	}

	// Handle some records
	ctx := context.Background()
	err = proc.Handle(ctx, &partition.RecordImpl{
		Data:         []byte("record1"),
		ID:           "t1",
		PartitionKey: "same",
		Timestamp:    time.Unix(1000, 0),
	})
	if err != nil {
		return
	}
	err = proc.Handle(ctx, &partition.RecordImpl{
		Data:         []byte("record2"),
		ID:           "t2",
		PartitionKey: "same",
		Timestamp:    time.Unix(1001, 0),
	})
	if err != nil {
		return
	}

	time.Sleep(time.Millisecond * 500)

	// Gracefully shutdown the processor
	if err := proc.Stop(); err != nil {
		fmt.Printf("Failed to stop processor: %v\n", err)
		return
	}

	// Output:
	// Processing record: t1
	// Processing record: t2
}
