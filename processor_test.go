package xp_test

import (
	"context"
	"io"
	"iter"
	"testing"
	"time"

	"github.com/davidvella/xp"
	"github.com/davidvella/xp/consumer"
	"github.com/davidvella/xp/handler"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/partition/strategy/messagecount"
	"github.com/davidvella/xp/processor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockProducerStorage implements processor.Storage for testing.
type MockProducerStorage struct {
	createFunc  func(context.Context, string) (io.WriteCloser, error)
	publishFunc func(context.Context, string) error
}

func (m *MockProducerStorage) List(_ context.Context) ([]string, error) {
	return nil, nil
}

func (m *MockProducerStorage) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, path)
	}
	return nil, nil
}

func (m *MockProducerStorage) Publish(ctx context.Context, path string) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, path)
	}
	return nil
}

// MockConsumerStorage implements consumer.Storage for testing.
type MockConsumerStorage struct {
	openFunc        func(context.Context, string) (consumer.ReadAtCloser, error)
	listPublishFunc func(context.Context) ([]string, error)
	deleteFunc      func(context.Context, string) error
}

func (m *MockConsumerStorage) Open(ctx context.Context, path string) (consumer.ReadAtCloser, error) {
	if m.openFunc != nil {
		return m.openFunc(ctx, path)
	}
	return nil, nil
}

func (m *MockConsumerStorage) ListPublished(ctx context.Context) ([]string, error) {
	if m.listPublishFunc != nil {
		return m.listPublishFunc(ctx)
	}
	return nil, nil
}

func (m *MockConsumerStorage) Delete(ctx context.Context, path string) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, path)
	}
	return nil
}

// MockHandler implements handler.Handler for testing.
type MockHandler struct {
	handleFunc func(context.Context, string, iter.Seq[partition.Record]) error
}

func (m *MockHandler) Handle(ctx context.Context, partitionKey string, records iter.Seq[partition.Record]) error {
	if m.handleFunc != nil {
		return m.handleFunc(ctx, partitionKey, records)
	}
	return nil
}

func TestNewProcessor(t *testing.T) {
	tests := []struct {
		name            string
		producerStorage processor.Storage
		consumerStorage consumer.Storage
		handler         handler.Handler
		opts            []xp.Option
		wantErr         bool
		errMsg          string
	}{
		{
			name:            "valid initialization",
			producerStorage: &MockProducerStorage{},
			consumerStorage: &MockConsumerStorage{},
			handler:         &MockHandler{},
			opts:            []xp.Option{xp.WithMaxConcurrency(1), xp.WithStrategy(messagecount.NewStrategy(1))},
			wantErr:         false,
		},
		{
			name:            "missing strategy",
			producerStorage: &MockProducerStorage{},
			consumerStorage: &MockConsumerStorage{},
			handler:         &MockHandler{},
			wantErr:         true,
			errMsg:          "xp: partition strategy is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc, err := xp.NewProcessor(tt.producerStorage, tt.consumerStorage, tt.handler, tt.opts...)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
				assert.Nil(t, proc)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, proc)
		})
	}
}

func TestProcessor_Handle(t *testing.T) {
	tests := []struct {
		name       string
		setupMocks func() (*MockProducerStorage, *MockConsumerStorage, *MockHandler)
		record     partition.Record
		wantErr    bool
		errMsg     string
	}{
		{
			name: "successful handle",
			setupMocks: func() (*MockProducerStorage, *MockConsumerStorage, *MockHandler) {
				return &MockProducerStorage{
					createFunc: func(_ context.Context, _ string) (io.WriteCloser, error) {
						return &MockWriteCloser{}, nil
					},
				}, &MockConsumerStorage{}, &MockHandler{}
			},
			record: partition.RecordImpl{
				ID:           "test-id",
				PartitionKey: "test-partition",
				Data:         []byte("test data"),
				Timestamp:    time.Now(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producerStorage, consumerStorage, h := tt.setupMocks()
			proc, err := xp.NewProcessor(
				producerStorage,
				consumerStorage,
				h,
				xp.WithStrategy(&MockStrategy{}),
			)
			require.NoError(t, err)

			err = proc.Handle(context.Background(), tt.record)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errMsg, err.Error())
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestProcessor_Stop(t *testing.T) {
	producerStorage := &MockProducerStorage{}
	consumerStorage := &MockConsumerStorage{}
	h := &MockHandler{}

	proc, err := xp.NewProcessor(
		producerStorage,
		consumerStorage,
		h,
		xp.WithStrategy(&MockStrategy{}),
	)
	require.NoError(t, err)

	// Test graceful shutdown
	err = proc.Stop()
	assert.NoError(t, err)
}

// MockWriteCloser is a helper for testing.
type MockWriteCloser struct {
	writeErr error
	closeErr error
}

func (m *MockWriteCloser) Write(p []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return len(p), nil
}

func (m *MockWriteCloser) Close() error {
	return m.closeErr
}

// MockStrategy is a helper for testing.
type MockStrategy struct{}

func (m *MockStrategy) ShouldRotate(partition.Information, time.Time) bool {
	return false
}
