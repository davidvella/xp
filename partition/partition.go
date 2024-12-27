package partition

import (
	"cmp"
	"time"
)

type RecordImpl struct {
	ID           string
	PartitionKey string
	Timestamp    time.Time
	Data         []byte
}

func (r RecordImpl) GetID() string {
	return r.ID
}

func (r RecordImpl) GetPartitionKey() string {
	return r.PartitionKey
}

func (r RecordImpl) GetWatermark() time.Time {
	return r.Timestamp
}

func (r RecordImpl) GetData() []byte {
	return r.Data
}

func (r RecordImpl) Less(t Record) bool {
	if c := cmp.Compare(r.PartitionKey, t.GetPartitionKey()); c < 0 {
		return true
	}

	if c := cmp.Compare(r.ID, t.GetID()); c < 0 {
		return true
	}

	if c := r.Timestamp.Compare(t.GetWatermark()); c < 0 {
		return true
	}

	return false
}

type Record interface {
	Less(t Record) bool
	GetID() string
	GetPartitionKey() string
	GetWatermark() time.Time
	GetData() []byte
}

type Strategy interface {
	ShouldRotate(information Information, watermark time.Time) bool
}

type Information struct {
	PartitionKey   string
	RecordCount    int
	FirstWatermark time.Time
}
