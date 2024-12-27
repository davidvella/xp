package sstable_test

import (
	"io"
	"os"
	"testing"

	"github.com/davidvella/xp/core/sstable"
	"github.com/stretchr/testify/assert"
)

func setupTestFile(t *testing.T) (f sstable.ReadWriteSeeker, cleanup func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "test-*.sst")
	assert.NoError(t, err)

	cleanup = func() {
		os.Remove(tmpFile.Name())
	}

	return tmpFile, cleanup
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		input    []byte
		expN     int
		expError error
	}{
		{
			name:  "write within buffer",
			size:  8,
			input: []byte("hello"),
			expN:  5,
		},
		{
			name:  "write larger than buffer",
			size:  4,
			input: []byte("hello world"),
			expN:  11,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, cleanup := setupTestFile(t)
			defer cleanup()
			rws := sstable.NewReadWriteSeeker(b, tc.size)
			n, err := rws.Write(tc.input)
			assert.Equal(t, tc.expError, err)
			assert.Equal(t, tc.expN, n)
		})
	}
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
			rws := sstable.NewReadWriteSeeker(b, tc.size)
			_, err := rws.Write(tc.write)
			assert.NoError(t, err)
			assert.NoError(t, rws.Flush())

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
			rws := sstable.NewReadWriteSeeker(b, tc.size)
			_, err := rws.Write(tc.write)
			assert.NoError(t, err)
			assert.NoError(t, rws.Flush())

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

func TestReadAt(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		write    []byte
		offset   int64
		readLen  int
		expData  []byte
		expLen   int
		expError error
	}{
		{
			name:    "read at start",
			size:    8,
			write:   []byte("hello world"),
			offset:  0,
			readLen: 5,
			expData: []byte("hello"),
			expLen:  5,
		},
		{
			name:    "read at middle",
			size:    8,
			write:   []byte("hello world"),
			offset:  6,
			readLen: 5,
			expData: []byte("world"),
			expLen:  5,
		},
		{
			name:     "read at with offset beyond size",
			size:     8,
			write:    []byte("hello"),
			offset:   10,
			readLen:  5,
			expData:  []byte{0, 0, 0, 0, 0},
			expError: io.EOF,
			expLen:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, cleanup := setupTestFile(t)
			defer cleanup()
			rws := sstable.NewReadWriteSeeker(b, tc.size)
			_, err := rws.Write(tc.write)
			assert.NoError(t, err)

			data := make([]byte, tc.readLen)
			n, err := rws.ReadAt(data, tc.offset)
			if tc.expError != nil {
				assert.ErrorIs(t, err, tc.expError)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.expLen, n)
			assert.Equal(t, tc.expData, data)
		})
	}
}

func TestFlush(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		writes   [][]byte
		expError error
	}{
		{
			name: "multiple writes then flush",
			size: 8,
			writes: [][]byte{
				[]byte("hello"),
				[]byte(" "),
				[]byte("world"),
			},
		},
		{
			name:   "flush empty buffer",
			size:   8,
			writes: [][]byte{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, cleanup := setupTestFile(t)
			defer cleanup()
			rws := sstable.NewReadWriteSeeker(b, tc.size)

			for _, write := range tc.writes {
				_, err := rws.Write(write)
				assert.NoError(t, err)
			}

			err := rws.Flush()
			assert.Equal(t, tc.expError, err)
		})
	}
}
