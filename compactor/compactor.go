package compactor

import (
	"io"
	"time"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

var (
	// Create a string with the maximum Unicode code point (U+10FFFF).
	maxPossibleString = "\U0010FFFF"
	// The max time that can be represented.
	maxTime   = time.Date(292277026596, 12, 4, 15, 30, 7, 999999999, time.UTC)
	maxRecord = partition.RecordImpl{
		ID:           maxPossibleString,
		PartitionKey: maxPossibleString,
		// The max time that can be represented
		Timestamp: maxTime,
	}
)

// Compact performs streaming compaction of multiple sequences using a loser tree.
func Compact(w io.Writer, sequences ...loser.Sequence[partition.Record]) error {
	if len(sequences) == 0 {
		return nil
	}

	var (
		lt   = loser.New[partition.Record](sequences, maxRecord)
		last partition.Record
		done bool
	)

	for current := range lt.All() {
		if !done {
			last = current
			done = true
			continue
		}
		if last != nil && current.GetID() != last.GetID() {
			_, err := recordio.Write(w, last)
			if err != nil {
				return err
			}
		}
		last = current
	}

	if done {
		_, err := recordio.Write(w, last)
		if err != nil {
			return err
		}
	}

	return nil
}
