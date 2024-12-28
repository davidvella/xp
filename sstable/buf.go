package sstable

import (
	"bufio"
	"io"
)

// ReaderWriterSeeker stores pointers to a [io.Reader] and a [io.Writer] and a [io.Seeker].
// It implements [ReadWriteSeeker].
type ReaderWriterSeeker struct {
	reader *bufio.Reader
	writer *bufio.Writer
	rws    io.ReadWriteSeeker
}

func NewReadWriteSeeker(r io.ReadWriteSeeker, size int) *ReaderWriterSeeker {
	return &ReaderWriterSeeker{
		reader: bufio.NewReaderSize(r, size),
		writer: bufio.NewWriterSize(r, size),
		rws:    r,
	}
}

func (r *ReaderWriterSeeker) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r *ReaderWriterSeeker) Write(p []byte) (n int, err error) {
	return r.writer.Write(p)
}

func (r *ReaderWriterSeeker) Flush() error {
	return r.writer.Flush()
}

func (r *ReaderWriterSeeker) Seek(offset int64, whence int) (int64, error) {
	if err := r.writer.Flush(); err != nil {
		return 0, err
	}

	pos, err := r.rws.Seek(offset, whence)
	if err != nil {
		return pos, err
	}

	r.reader.Reset(r.rws)
	r.writer.Reset(r.rws)
	return pos, nil
}
