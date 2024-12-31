package handler

import (
	"context"
	"iter"

	"github.com/davidvella/xp/partition"
)

// Handler defines the interface for processing windows.
type Handler interface {
	// Handle processes a single window
	Handle(ctx context.Context, partitionKey string, window iter.Seq[partition.Record]) error
}

// Func is a function type that implements Handler.
type Func func(ctx context.Context, partitionKey string, window iter.Seq[partition.Record]) error

// Handle calls the function.
func (f Func) Handle(ctx context.Context, partitionKey string, window iter.Seq[partition.Record]) error {
	return f(ctx, partitionKey, window)
}
