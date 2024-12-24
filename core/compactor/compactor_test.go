package compactor

import (
	"bytes"
	"iter"
	"testing"
	"time"

	"github.com/davidvella/xp/core/loser"
	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/recordio"
	"github.com/stretchr/testify/assert"
)

type List[E loser.Lesser[E]] struct {
	list []E
	cur  E
}

func NewList[E loser.Lesser[E]](list ...E) *List[E] {
	return &List[E]{list: list}
}

func (it *List[E]) All() iter.Seq[E] {
	return func(yield func(E) bool) {
		for _, i := range it.list {
			yield(i)
		}
	}
}

func TestCompact(t *testing.T) {
	type args struct {
		sequences []loser.Sequence[partition.Record]
	}
	tests := []struct {
		name        string
		args        args
		wantRecords []partition.Record
		wantErr     bool
	}{
		{
			name: "Compact across multiple sequences",
			args: args{
				[]loser.Sequence[partition.Record]{
					NewList[partition.Record](
						partition.Record{
							ID:           "123",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC),
							Data:         []byte{},
						},
						partition.Record{
							ID:           "124",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 2, time.UTC),
							Data:         []byte{},
						}),
					NewList[partition.Record](
						partition.Record{
							ID:           "123",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC),
							Data:         []byte{},
						}),
				},
			},
			wantRecords: []partition.Record{
				{
					ID:           "123",
					PartitionKey: "",
					Timestamp:    time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC),
					Data:         []byte{},
				},
				{
					ID:           "124",
					PartitionKey: "",
					Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 2, time.UTC),
					Data:         []byte{},
				},
			},
			wantErr: false,
		},
		{
			name: "Handles empty sequences",
			args: args{
				[]loser.Sequence[partition.Record]{
					NewList[partition.Record](),
				},
			},
			wantRecords: []partition.Record{},
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			err := Compact(w, tt.args.sequences...)
			if tt.wantErr {
				assert.Error(t, err)
			}
			gotRecords := recordio.ReadRecords(w)
			assert.ElementsMatch(t, tt.wantRecords, gotRecords)
		})
	}
}
