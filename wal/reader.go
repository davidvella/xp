package wal

import (
	"errors"
	"io"
	"iter"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

type Reader struct {
	r        io.ReaderAt
	segments []offset
}

func NewReader(r io.ReaderAt) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) All() iter.Seq[partition.Record] {
	err := r.readExistingSegments()
	if err != nil {
		return nil
	}

	var sequences = make([]loser.Sequence[partition.Record], 0, len(r.segments))

	for _, seg := range r.segments {
		reader := &segmentReader{
			reader: r.r,
			offset: seg.offset,
			length: seg.length,
		}
		sequences = append(sequences, reader)
	}

	tree := loser.New(sequences, partition.Max, partition.Less)
	return tree.All()
}

func (r *Reader) readExistingSegments() error {
	o := int64(0)
	for {
		reader := io.NewSectionReader(r.r, o, recordio.Int64Size)
		seg, err := readSegment(reader, o)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		r.segments = append(r.segments, seg)
		o += seg.length
	}
	return nil
}

func readSegment(r io.Reader, o int64) (offset, error) {
	br := recordio.NewBinaryReader(r)
	l, err := br.ReadInt64()
	if err != nil {
		return offset{}, err
	}

	return offset{
		offset: o,
		length: l,
	}, nil
}

type offset struct {
	offset int64
	length int64
}

type segmentReader struct {
	reader io.ReaderAt
	offset int64
	length int64
}

func (sr *segmentReader) All() iter.Seq[partition.Record] {
	reader := io.NewSectionReader(sr.reader, sr.offset+recordio.Int64Size, sr.length-recordio.Int64Size)
	return recordio.Seq(reader)
}
