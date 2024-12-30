package compactor

import (
	"fmt"
	"io"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
)

// Compact performs streaming compaction of multiple sequences using a loser tree.
func Compact(w io.WriteSeeker, sequences ...loser.Sequence[partition.Record]) error {
	if len(sequences) == 0 {
		return nil
	}

	writer, err := sstable.OpenWriter(w, nil)
	if err != nil {
		return fmt.Errorf("compactor: failed to open table: %w", err)
	}

	var (
		lt   = loser.New[partition.Record](sequences, partition.Max, partition.Less)
		last partition.Record
	)

	for current := range lt.All() {
		if last != nil && current.GetID() != last.GetID() {
			if err := writer.Write(last); err != nil {
				return err
			}
		}
		last = current
	}

	if last != nil {
		if err := writer.Write(last); err != nil {
			return err
		}
	}

	return writer.Close()
}
