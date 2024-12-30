package sstable

import (
	"bufio"
	"io"
)

// ReaderSeeker stores pointers to a [io.Reader] and a [io.Writer] and a [io.Seeker].
// It implements [ReadWriteSeeker].
type ReaderSeeker struct {
	reader *bufio.Reader
	rws    io.ReadSeeker
}

func NewReadSeeker(r io.ReadSeeker, size int) *ReaderSeeker {
	return &ReaderSeeker{
		reader: bufio.NewReaderSize(r, size),
		rws:    r,
	}
}

func (r *ReaderSeeker) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *ReaderSeeker) Seek(offset int64, whence int) (int64, error) {
	pos, err := r.rws.Seek(offset, whence)
	if err != nil {
		return pos, err
	}

	r.reader.Reset(r.rws)
	return pos, nil
}
