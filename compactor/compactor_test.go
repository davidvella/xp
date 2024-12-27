package compactor_test

import (
	"bytes"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/davidvella/xp/compactor"
	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
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
			w := &bytes.Buffer{}
			err := compactor.Compact(w, tt.args.sequences...)
			if tt.wantErr {
				assert.Error(t, err)
			}
			gotRecords := recordio.ReadRecords(w)
			assert.ElementsMatch(t, tt.wantRecords, gotRecords)
		})
	}
}

func TestCompactHandleError(t *testing.T) {
	var records = []loser.Sequence[partition.Record]{
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
	}
	w := &mockWriter{
		errorCounter: 2,
	}
	err := compactor.Compact(w, records...)
	assert.Error(t, err)
}

func TestCompactHandleErrorAtEnd(t *testing.T) {
	var records = []loser.Sequence[partition.Record]{
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
	}
	w := &mockWriter{
		errorCounter: 11,
	}
	err := compactor.Compact(w, records...)
	assert.Error(t, err)
}

func TestCompactHandleNoSequences(t *testing.T) {
	w := &mockWriter{
		errorCounter: 11,
	}
	err := compactor.Compact(w)
	assert.NoError(t, err)
}

var errWrite = errors.New("its a me, error")

type mockWriter struct {
	errorCounter int
	counter      int
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	w.counter++
	if w.counter == w.errorCounter {
		return 0, errWrite
	}
	return len(p), nil
}
