package compactor_test

import (
	"iter"
	"os"
	"testing"
	"time"

	"github.com/davidvella/xp/compactor"
	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
	"github.com/stretchr/testify/assert"
)

type List[E loser.Lesser[E]] struct {
	list []E
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
		wantRecords []partition.RecordImpl
		wantErr     bool
	}{
		{
			name: "Compact across multiple sequences",
			args: args{
				[]loser.Sequence[partition.Record]{
					NewList[partition.Record](
						partition.RecordImpl{
							ID:           "123",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC),
							Data:         []byte{},
						},
						partition.RecordImpl{
							ID:           "124",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 2, time.UTC),
							Data:         []byte{},
						}),
					NewList[partition.Record](
						partition.RecordImpl{
							ID:           "123",
							PartitionKey: "",
							Timestamp:    time.Date(2024, 1, 1, 0, 0, 2, 0, time.UTC),
							Data:         []byte{},
						}),
				},
			},
			wantRecords: []partition.RecordImpl{
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
			wantRecords: []partition.RecordImpl{},
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.sst")
			assert.NoError(t, err)
			err = compactor.Compact(tmpFile, tt.args.sequences...)
			if tt.wantErr {
				assert.Error(t, err)
			}

			table, err := sstable.Open(tmpFile, nil)
			assert.NoError(t, err)

			var gotRecords []partition.Record
			for record := range table.All() {
				gotRecords = append(gotRecords, record)
			}
			assert.ElementsMatch(t, tt.wantRecords, gotRecords)
		})
	}
}

func TestCompactHandleNoSequences(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "test-*.sst")
	assert.NoError(t, err)
	err = compactor.Compact(tmpFile)
	assert.NoError(t, err)
}

func TestCompactHandleErrorCreatingTable(t *testing.T) {
	err := compactor.Compact(nil, NewList[partition.Record]())
	assert.Error(t, err)
}
