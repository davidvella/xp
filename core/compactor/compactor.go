package compactor

import (
	"io"
	"time"

	"github.com/davidvella/xp/core/loser"
	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/recordio"
)

var (
	// Create a string with the maximum Unicode code point (U+10FFFF)
	maxPossibleString = "\U0010FFFF"
	// The max time that can be represented
	maxTime   = time.Date(292277026596, 12, 4, 15, 30, 7, 999999999, time.UTC)
	maxRecord = partition.Record{
		ID:           maxPossibleString,
		PartitionKey: maxPossibleString,
		// The max time that can be represented
		Timestamp: maxTime,
	}
)

// Compact performs streaming compaction of multiple sequences using a loser tree
func Compact(w io.Writer, sequences ...loser.Sequence[partition.Record]) error {
	if len(sequences) == 0 {
		return nil
	}

	var (
		lt   = loser.New(sequences, maxRecord)
		last partition.Record
		done bool
	)

	for current := range lt.All() {
		if !done {
			last = current
			done = true
			continue
		}
		if current.ID != last.ID {
			err := recordio.Write(w, last)
			if err != nil {
				return err
			}
		}
		last = current
	}

	if done {
		err := recordio.Write(w, last)
		if err != nil {
			return err
		}
	}

	return nil
}
