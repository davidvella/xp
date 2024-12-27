package sstable

import (
	"bufio"
	"io"
)

type ReadWriteSeeker interface {
	io.Reader
	io.ReaderAt
	io.Writer
	io.Seeker
}

// ReaderWriterSeeker stores pointers to a [io.Reader] and a [io.Writer] and a [io.Seeker].
// It implements [ReadWriteSeeker].
type ReaderWriterSeeker struct {
	reader *bufio.Reader
	writer *bufio.Writer
	rws    ReadWriteSeeker
}

func NewReadWriteSeeker(r ReadWriteSeeker, size int) *ReaderWriterSeeker {
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

func (r *ReaderWriterSeeker) ReadAt(p []byte, off int64) (n int, err error) {
	if err := r.Flush(); err != nil {
		return 0, err
	}

	currentPos, err := r.rws.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}

	n, err = r.rws.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	_, err = r.rws.Seek(currentPos, io.SeekStart)
	return n, err
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
