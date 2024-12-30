package sstable

import (
	"bufio"
	"io"
)

// BufferReaderSeeker stores pointers to a [io.Reader] and a [io.Writer] and a [io.Seeker].
// It implements [ReadWriteSeeker].
type BufferReaderSeeker struct {
	reader *bufio.Reader
	rws    io.ReadSeeker
}

func NewReadSeeker(r io.ReadSeeker, size int) *BufferReaderSeeker {
	return &BufferReaderSeeker{
		reader: bufio.NewReaderSize(r, size),
		rws:    r,
	}
}

func (r *BufferReaderSeeker) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *BufferReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	pos, err := r.rws.Seek(offset, whence)
	if err != nil {
		return pos, err
	}

	r.reader.Reset(r.rws)
	return pos, nil
}
