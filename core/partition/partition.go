package partition

import (
	"cmp"
	"time"
)

type Record struct {
	ID           string
	PartitionKey string
	Timestamp    time.Time
	Data         []byte
}

func (r Record) Less(t Record) bool {
	if c := cmp.Compare(r.PartitionKey, t.PartitionKey); c < 0 {
		return true
	}

	if c := cmp.Compare(r.ID, t.ID); c < 0 {
		return true
	}

	if c := r.Timestamp.Compare(t.Timestamp); c < 0 {
		return true
	}

	return false
}

type Strategy interface {
	ShouldRotate(first, incoming Record) bool
}
