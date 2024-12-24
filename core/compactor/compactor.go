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
	maxRecord         = partition.Record{
		ID:           maxPossibleString,
		PartitionKey: maxPossibleString,
		// The max time that can be represented
		Timestamp: time.Date(292277026596, 12, 4, 15, 30, 7, 999999999, time.UTC),
	}
)

// Compact performs streaming compaction of multiple sequences using a loser tree
func Compact(w io.Writer, sequences ...loser.Sequence[partition.Record]) error {
	if len(sequences) == 0 {
		return nil
	}

	lt := loser.New(sequences, maxRecord)

	var (
		last partition.Record
		once bool
	)

	for current := range lt.All() {
		if !once {
			last = current
			once = true
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

	if once {
		err := recordio.Write(w, last)
		if err != nil {
			return err
		}
	}

	return nil
}
