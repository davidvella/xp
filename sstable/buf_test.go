package sstable_test

import (
	"io"
	"os"
	"testing"

	"github.com/davidvella/xp/sstable"
	"github.com/stretchr/testify/assert"
)

func setupTestFile(t *testing.T) (f io.ReadWriteSeeker, cleanup func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.sst")
	assert.NoError(t, err)

	cleanup = func() {
		os.Remove(tmpFile.Name())
	}

	return tmpFile, cleanup
}

func TestRead(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		write    []byte
		readLen  int
		expData  []byte
		expError error
	}{
		{
			name:    "read exact buffer",
			size:    5,
			write:   []byte("hello"),
			readLen: 5,
			expData: []byte("hello"),
		},
		{
			name:    "read partial buffer",
			size:    8,
			write:   []byte("hello world"),
			readLen: 5,
			expData: []byte("hello"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, cleanup := setupTestFile(t)
			defer cleanup()
			_, err := b.Write(tc.write)
			assert.NoError(t, err)

			rws := sstable.NewReadSeeker(b, tc.size)

			_, err = rws.Seek(0, io.SeekStart)
			assert.NoError(t, err)

			data := make([]byte, tc.readLen)
			n, err := rws.Read(data)
			assert.Equal(t, tc.expError, err)
			assert.Equal(t, len(tc.expData), n)
			assert.Equal(t, tc.expData, data)
		})
	}
}

func TestSeek(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		write    []byte
		offset   int64
		whence   int
		expPos   int64
		expRead  []byte
		expError error
	}{
		{
			name:    "seek start",
			size:    8,
			write:   []byte("hello world"),
			offset:  0,
			whence:  io.SeekStart,
			expPos:  0,
			expRead: []byte("hello"),
		},
		{
			name:    "seek current",
			size:    8,
			write:   []byte("hello world"),
			offset:  6,
			whence:  io.SeekStart,
			expPos:  6,
			expRead: []byte("world"),
		},
		{
			name:    "seek end",
			size:    8,
			write:   []byte("hello world"),
			offset:  -5,
			whence:  io.SeekEnd,
			expPos:  6,
			expRead: []byte("world"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, cleanup := setupTestFile(t)
			defer cleanup()
			_, err := b.Write(tc.write)
			assert.NoError(t, err)

			rws := sstable.NewReadSeeker(b, tc.size)

			pos, err := rws.Seek(tc.offset, tc.whence)
			assert.Equal(t, tc.expError, err)
			assert.Equal(t, tc.expPos, pos)

			if len(tc.expRead) > 0 {
				data := make([]byte, len(tc.expRead))
				n, err := rws.Read(data)
				assert.NoError(t, err)
				assert.Equal(t, len(tc.expRead), n)
				assert.Equal(t, tc.expRead, data)
			}
		})
	}
}
