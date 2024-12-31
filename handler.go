package xp

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

// HandlerFunc is a function type that implements Handler.
type HandlerFunc func(ctx context.Context, partitionKey string, window iter.Seq[partition.Record]) error

// Handle calls the function.
func (f HandlerFunc) Handle(ctx context.Context, partitionKey string, window iter.Seq[partition.Record]) error {
	return f(ctx, partitionKey, window)
}
