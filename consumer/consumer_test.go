package consumer_test

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/davidvella/xp/consumer"
	"github.com/davidvella/xp/partition"
	"github.com/stretchr/testify/assert"
)

// MockStorage implements Storage for testing.
type MockStorage struct {
	files     map[string][]byte
	openErr   error
	listErr   error
	deleteErr error
	mu        sync.Mutex
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		files: make(map[string][]byte),
	}
}

func (m *MockStorage) Open(_ context.Context, path string) (consumer.ReadAtCloser, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	m.mu.Lock()
	data, exists := m.files[path]
	m.mu.Unlock()
	if !exists {
		return nil, errors.New("file not found")
	}
	return &MockReader{data: data}, nil
}

func (m *MockStorage) ListPublished(_ context.Context) ([]string, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make([]string, 0, len(m.files))
	for k := range m.files {
		files = append(files, k)
	}
	return files, nil
}

func (m *MockStorage) Delete(_ context.Context, path string) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	m.mu.Lock()
	delete(m.files, path)
	m.mu.Unlock()
	return nil
}

// MockReader implements ReadAtCloser for testing.
type MockReader struct {
	data []byte
}

func (m *MockReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n = copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *MockReader) Close() error {
	return nil
}

// MockHandler implements Handler for testing.
type MockHandler struct {
	handleFunc func(ctx context.Context, partitionKey string, seq iter.Seq[partition.Record]) error
}

func (m *MockHandler) Handle(ctx context.Context, partitionKey string, seq iter.Seq[partition.Record]) error {
	if m.handleFunc != nil {
		return m.handleFunc(ctx, partitionKey, seq)
	}
	return nil
}

func TestConsumer_Process(t *testing.T) {
	tests := []struct {
		name          string
		setupStorage  func() *MockStorage
		setupHandler  func() *MockHandler
		expectedError string
	}{
		{
			name: "successful processing",
			setupStorage: func() *MockStorage {
				ms := NewMockStorage()
				ms.files["partition1_1704067201.wal"] = []byte("test data")
				return ms
			},
			setupHandler: func() *MockHandler {
				return &MockHandler{
					handleFunc: func(_ context.Context, _ string, _ iter.Seq[partition.Record]) error {
						return nil
					},
				}
			},
		},
		{
			name: "list error",
			setupStorage: func() *MockStorage {
				ms := NewMockStorage()
				ms.listErr = errors.New("list error")
				return ms
			},
			setupHandler: func() *MockHandler {
				return &MockHandler{}
			},
			expectedError: "failed to list published files: list error",
		},
		{
			name: "open error",
			setupStorage: func() *MockStorage {
				ms := NewMockStorage()
				ms.files["partition1_1704067201.wal"] = []byte("test data")
				ms.openErr = errors.New("open error")
				return ms
			},
			setupHandler: func() *MockHandler {
				return &MockHandler{}
			},
			expectedError: "failed to process file partition1_1704067201.wal: failed to open file: open error",
		},
		{
			name: "handler error",
			setupStorage: func() *MockStorage {
				ms := NewMockStorage()
				ms.files["partition1_1704067201.wal"] = []byte("test data")
				return ms
			},
			setupHandler: func() *MockHandler {
				return &MockHandler{
					handleFunc: func(_ context.Context, _ string, _ iter.Seq[partition.Record]) error {
						return errors.New("handler error")
					},
				}
			},
			expectedError: "failed to process file partition1_1704067201.wal: handler error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			handler := tt.setupHandler()

			c := consumer.New(storage, handler, consumer.DefaultOptions())
			err := c.Process(context.Background())

			if tt.expectedError != "" {
				assert.EqualError(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConsumer_StartStop(t *testing.T) {
	storage := NewMockStorage()
	storage.files["partition1_1704067201.wal"] = []byte("test data")

	processed := make(chan struct{})
	handler := &MockHandler{
		handleFunc: func(_ context.Context, _ string, _ iter.Seq[partition.Record]) error {
			processed <- struct{}{}
			return nil
		},
	}

	opts := consumer.Options{
		PollInterval:   100 * time.Millisecond,
		MaxConcurrency: 1,
	}

	c := consumer.New(storage, handler, opts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumer in background
	go func() {
		err := c.Start(ctx)
		assert.NoError(t, err)
	}()

	// Wait for first processing
	select {
	case <-processed:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for processing")
	}

	// Stop consumer
	c.Stop()

	// Verify no more processing occurs
	select {
	case <-processed:
		t.Fatal("processing occurred after stop")
	case <-time.After(200 * time.Millisecond):
	}
}
