package compactor

import (
	"fmt"
	"io"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
)

// Compact performs streaming compaction of multiple sequences using a loser tree.
func Compact(w io.ReadWriteSeeker, sequences ...loser.Sequence[partition.Record]) error {
	if len(sequences) == 0 {
		return nil
	}

	sst, err := sstable.Open(w, nil)
	if err != nil {
		return fmt.Errorf("compactor: failed to open table: %w", err)
	}

	var (
		lt   = loser.New[partition.Record](sequences, partition.Max, partition.Less)
		last partition.Record
		bw   = sst.BatchWriter()
		done bool
	)

	for current := range lt.All() {
		if !done {
			last = current
			done = true
			continue
		}
		if last != nil && current.GetID() != last.GetID() {
			if err := bw.Add(last); err != nil {
				return err
			}
		}
		last = current
	}

	if done {
		if err := bw.Add(last); err != nil {
			return err
		}
	}

	return bw.Close()
}
