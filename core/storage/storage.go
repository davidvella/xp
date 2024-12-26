package storage

import (
	"context"
	"io"
)

// Storage defines the interface for the underlying storage system.
type Storage interface {
	// Create a new file/object for writing
	Create(ctx context.Context, path string) (io.WriteCloser, error)
	// Publish a file/object from pending to publishing
	Publish(ctx context.Context, path string) error
	// List files/objects from pending
	List(ctx context.Context) ([]string, error)
}
