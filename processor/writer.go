package processor

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type WalKey struct {
	PartitionKey string
	Watermark    time.Time
}

func Serialize(w WalKey) string {
	return fmt.Sprintf("%s_%d.wal", w.PartitionKey, w.Watermark.Unix())
}

func Deserialize(name string) (WalKey, error) {
	// Remove .wal extension
	name = strings.TrimSuffix(name, ".wal")

	parts := strings.Split(name, "_")
	if len(parts) != 2 {
		return WalKey{}, fmt.Errorf("invalid WAL writer name format")
	}

	timestamp, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return WalKey{}, fmt.Errorf("invalid timestamp: %w", err)
	}

	return WalKey{
		PartitionKey: parts[0],
		Watermark:    time.Unix(timestamp, 0),
	}, nil
}
