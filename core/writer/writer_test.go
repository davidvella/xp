package writer

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
)

// MockStorage implements storage.Storage interface
type MockStorage struct {
	createFunc  func(ctx context.Context, path string) (io.WriteCloser, error)
	publishFunc func(ctx context.Context, path string) error
	listFunc    func(ctx context.Context) ([]string, error)
}

func (m *MockStorage) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, path)
	}
	return nil, fmt.Errorf("Create not implemented")
}

func (m *MockStorage) Publish(ctx context.Context, path string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, path)
	}
	return fmt.Errorf("Publish not implemented")
}

func (m *MockStorage) List(ctx context.Context) ([]string, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx)
	}
	return nil, fmt.Errorf("List not implemented")
}

// MockStrategy implements partition.Strategy interface
type MockStrategy struct {
	shouldRotateFunc func(first, current partition.Record) bool
}

func (m *MockStrategy) ShouldRotate(first, current partition.Record) bool {
	if m.shouldRotateFunc != nil {
		return m.shouldRotateFunc(first, current)
	}
	return false
}

// MockWriteCloser implements io.WriteCloser
type MockWriteCloser struct {
	writeFunc func(p []byte) (n int, err error)
	closeFunc func() error
}

func (m *MockWriteCloser) Write(p []byte) (n int, err error) {
	if m.writeFunc != nil {
		return m.writeFunc(p)
	}
	return 0, fmt.Errorf("Write not implemented")
}

func (m *MockWriteCloser) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return fmt.Errorf("Close not implemented")
}

func TestWriter_Write(t *testing.T) {
	tests := []struct {
		name          string
		record        partition.Record
		setupMocks    func() (*MockStorage, *MockStrategy)
		expectedError error
	}{
		{
			name: "successful write to new file",
			record: partition.Record{
				PartitionKey: "test",
				Timestamp:    time.Now(),
				Data:         []byte("test data"),
			},
			setupMocks: func() (*MockStorage, *MockStrategy) {
				writer := &MockWriteCloser{
					writeFunc: func(p []byte) (int, error) {
						return len(p), nil
					},
				}

				storage := &MockStorage{
					createFunc: func(ctx context.Context, path string) (io.WriteCloser, error) {
						return writer, nil
					},
				}

				strategy := &MockStrategy{
					shouldRotateFunc: func(first, current partition.Record) bool {
						return false
					},
				}

				return storage, strategy
			},
		},
		{
			name: "failed to create file",
			record: partition.Record{
				PartitionKey: "test",
				Timestamp:    time.Now(),
				Data:         []byte("test data"),
			},
			setupMocks: func() (*MockStorage, *MockStrategy) {
				storage := &MockStorage{
					createFunc: func(ctx context.Context, path string) (io.WriteCloser, error) {
						return nil, fmt.Errorf("storage error")
					},
				}
				strategy := &MockStrategy{}
				return storage, strategy
			},
			expectedError: fmt.Errorf("failed to create file: storage error"),
		},
		{
			name: "rotation needed and successful",
			record: partition.Record{
				PartitionKey: "test",
				Timestamp:    time.Now(),
				Data:         []byte("test data"),
			},
			setupMocks: func() (*MockStorage, *MockStrategy) {
				writer := &MockWriteCloser{
					writeFunc: func(p []byte) (int, error) {
						return len(p), nil
					},
					closeFunc: func() error {
						return nil
					},
				}

				storage := &MockStorage{
					createFunc: func(ctx context.Context, path string) (io.WriteCloser, error) {
						return writer, nil
					},
					publishFunc: func(ctx context.Context, path string) error {
						return nil
					},
				}

				strategy := &MockStrategy{
					shouldRotateFunc: func(first, current partition.Record) bool {
						return true
					},
				}

				return storage, strategy
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, strategy := tt.setupMocks()
			writer := New(storage, strategy)

			err := writer.Write(context.Background(), tt.record)

			if tt.expectedError != nil {
				if err == nil {
					t.Errorf("expected error %v, got nil", tt.expectedError)
				} else if err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestWriter_Close(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func() (*MockStorage, *MockStrategy)
		expectedError error
		preWrite      bool // whether to write before closing
	}{
		{
			name:     "successful close with active files",
			preWrite: true,
			setupMocks: func() (*MockStorage, *MockStrategy) {
				writer := &MockWriteCloser{
					writeFunc: func(p []byte) (int, error) {
						return len(p), nil
					},
					closeFunc: func() error {
						return nil
					},
				}

				storage := &MockStorage{
					createFunc: func(ctx context.Context, path string) (io.WriteCloser, error) {
						return writer, nil
					},
					publishFunc: func(ctx context.Context, path string) error {
						return nil
					},
				}

				strategy := &MockStrategy{}
				return storage, strategy
			},
		},
		{
			name:     "close with no active files",
			preWrite: false,
			setupMocks: func() (*MockStorage, *MockStrategy) {
				return &MockStorage{}, &MockStrategy{}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, strategy := tt.setupMocks()
			writer := New(storage, strategy)

			if tt.preWrite {
				record := partition.Record{
					PartitionKey: "test",
					Timestamp:    time.Now(),
					Data:         []byte("test data"),
				}
				if err := writer.Write(context.Background(), record); err != nil {
					t.Fatalf("failed to write test record: %v", err)
				}
			}

			err := writer.Close(context.Background())

			if tt.expectedError != nil {
				if err == nil {
					t.Errorf("expected error %v, got nil", tt.expectedError)
				} else if err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestWriter_Recover(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func() (*MockStorage, *MockStrategy)
		expectedError error
	}{
		{
			name: "successful recovery",
			setupMocks: func() (*MockStorage, *MockStrategy) {
				storage := &MockStorage{
					listFunc: func(ctx context.Context) ([]string, error) {
						return []string{"file1.dat", "file2.dat"}, nil
					},
					publishFunc: func(ctx context.Context, path string) error {
						return nil
					},
				}
				return storage, &MockStrategy{}
			},
		},
		{
			name: "list error",
			setupMocks: func() (*MockStorage, *MockStrategy) {
				storage := &MockStorage{
					listFunc: func(ctx context.Context) ([]string, error) {
						return nil, fmt.Errorf("list error")
					},
				}
				return storage, &MockStrategy{}
			},
			expectedError: fmt.Errorf("failed to list pending files: list error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage, strategy := tt.setupMocks()
			writer := New(storage, strategy)

			err := writer.Recover(context.Background())

			if tt.expectedError != nil {
				if err == nil {
					t.Errorf("expected error %v, got nil", tt.expectedError)
				} else if err.Error() != tt.expectedError.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedError, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
