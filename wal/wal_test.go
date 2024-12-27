package wal

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
	"github.com/stretchr/testify/assert"
)

// mockWriteCloser implements io.WriteCloser for testing.
type mockWriteCloser struct {
	writeErr error
	closeErr error
	written  []byte
	closed   bool
	mu       sync.Mutex
}

func (m *mockWriteCloser) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.written = append(m.written, p...)
	return len(p), nil
}

func (m *mockWriteCloser) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return m.closeErr
}

func TestWAL_Write(t *testing.T) {
	tests := []struct {
		name       string
		record     partition.Record
		writeErr   error
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "successful write",
			record:   partition.RecordImpl{Data: []byte("test data")},
			writeErr: nil,
			wantErr:  false,
		},
		{
			name:       "write error",
			record:     partition.RecordImpl{Data: []byte("test data")},
			writeErr:   errors.New("write failed"),
			wantErr:    true,
			wantErrMsg: "failed to write record: error writing ID: error writing string length: write failed",
		},
		{
			name:     "empty record",
			record:   partition.RecordImpl{Data: []byte{}},
			writeErr: nil,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockWriteCloser{writeErr: tt.writeErr}
			w := &Writer{w: mock}

			err := w.Write(tt.record)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantErrMsg != "" {
					assert.Equal(t, tt.wantErrMsg, err.Error())
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWAL_Close(t *testing.T) {
	tests := []struct {
		name     string
		closeErr error
		wantErr  bool
	}{
		{
			name:     "successful close",
			closeErr: nil,
			wantErr:  false,
		},
		{
			name:     "close error",
			closeErr: errors.New("close failed"),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &mockWriteCloser{closeErr: tt.closeErr}
			w := &Writer{w: mock}

			err := w.Close()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.closeErr, err)
			} else {
				assert.NoError(t, err)
			}
			assert.True(t, mock.closed, "underlying WriteCloser should be closed")
		})
	}
}

func TestWAL_Concurrent(t *testing.T) {
	mock := &mockWriteCloser{}
	w := &Writer{w: mock}

	// Test concurrent writes
	const numGoroutines = 10
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	var records = make([]partition.Record, 0, numGoroutines)
	for i := range numGoroutines {
		records = append(records, partition.RecordImpl{Data: []byte{byte(i)}, Timestamp: time.Now()})
	}

	for i := 0; i < numGoroutines; i++ {
		go func(i int) {
			defer wg.Done()
			err := w.Write(records[i])
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	assert.Equal(t, numGoroutines*47, len(mock.written))

	r := bytes.NewReader(mock.written)
	got := recordio.ReadRecords(r)

	assert.Len(t, got, numGoroutines)
}
