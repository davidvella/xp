package wal

import (
	"errors"
	"io"
	"iter"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
	"github.com/google/btree"
)

type Reader struct {
	r        io.ReaderAt
	segments []*segment
}

func NewReader(r io.ReaderAt) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) ReadAll() iter.Seq[partition.Record] {
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

	tree := loser.New(sequences, partition.Max)
	return tree.All()
}

func (r *Reader) readExistingSegments() error {
	offset := int64(0)
	for {
		reader := io.NewSectionReader(r.r, offset, recordio.Int64Size)
		seg, err := readSegment(reader, offset)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		r.segments = append(r.segments, seg)
		offset += seg.length
	}
	return nil
}

func readSegment(r io.Reader, offset int64) (*segment, error) {
	br := recordio.NewBinaryReader(r)
	l, err := br.ReadInt64()
	if err != nil {
		return nil, err
	}

	seg := newSegment()
	seg.offset = offset
	seg.length = l
	seg.flushed = true

	return seg, nil
}

func newSegment() *segment {
	return &segment{
		records: btree.NewG[partition.Record](2, func(a, b partition.Record) bool {
			return a.Less(b)
		}),
	}
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
