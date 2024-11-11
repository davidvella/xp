package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/davidvella/xp/core/serialization"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/types"
)

// Storage implements storage interface using Pebble
type Storage[K types.GroupKey, T any] struct {
	db                 *pebble.DB
	keySerializer      *serialization.KeySerializer
	valueSerializer    serialization.TypeSerializer[T]
	groupKeySerializer serialization.TypeSerializer[K]
	metadataSerializer serialization.TypeSerializer[WindowMetadata]
	batchSize          int
}

// type StorageOptions struct { configures the storage
type StorageOptions struct {
	Path         string
	BatchSize    int
	CacheSize    int64
	MaxOpenFiles int
}

func NewStorage[K types.GroupKey, T any](
	opts StorageOptions,
	valueSerializer serialization.TypeSerializer[T],
	groupKeySerializer serialization.TypeSerializer[K],
) (*Storage[K, T], error) {
	// Configure Pebble options
	pebbleOpts := &pebble.Options{
		Cache:        pebble.NewCache(opts.CacheSize),
		MaxOpenFiles: opts.MaxOpenFiles,
	}

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(opts.Path), 0755); err != nil {
		return nil, err
	}

	// Open database
	db, err := pebble.Open(opts.Path, pebbleOpts)
	if err != nil {
		return nil, err
	}

	return &Storage[K, T]{
		db:                 db,
		keySerializer:      &serialization.KeySerializer{},
		valueSerializer:    valueSerializer,
		groupKeySerializer: groupKeySerializer,
		metadataSerializer: serialization.NewGobSerializer[WindowMetadata](),
		batchSize:          opts.BatchSize,
	}, nil
}

func (p *Storage[K, T]) Close() error {
	return p.db.Close()
}

func (p *Storage[K, T]) SaveWindow(ctx context.Context, state storage.WindowState[K, T]) error {
	batch := p.db.NewBatch()
	defer batch.Close()

	// Save metadata
	metadata := WindowMetadata{
		WindowStart:    state.WindowStart,
		WindowEnd:      state.WindowEnd,
		LastUpdateTime: time.Now(),
	}

	metadataKey := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		state.WindowStart,
		[]byte{},
	)

	metadataBytes, err := p.metadataSerializer.SerializeValue(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize metadata: %w", err)
	}

	if err := batch.Set(metadataKey, metadataBytes, nil); err != nil {
		return err
	}

	// Save groups
	for groupKey, records := range state.Groups {
		// Serialize group key
		groupKeyBytes, err := p.groupKeySerializer.SerializeValue(groupKey)
		if err != nil {
			return fmt.Errorf("failed to serialize group key: %w", err)
		}

		// Split records into chunks
		for i := 0; i < len(records); i += p.batchSize {
			end := i + p.batchSize
			if end > len(records) {
				end = len(records)
			}

			// Serialize chunk of records
			chunkBytes, err := p.valueSerializer.SerializeChunk(records[i:end])
			if err != nil {
				return fmt.Errorf("failed to serialize chunk: %w", err)
			}

			// Create key with chunk index
			key := p.keySerializer.SerializeKey(
				string(WindowStateNamespace),
				state.WindowStart,
				append(groupKeyBytes, byte(i/p.batchSize)),
			)

			if err := batch.Set(key, chunkBytes, nil); err != nil {
				return err
			}

			// Commit batch if it gets too large
			if batch.Len() > 1000 {
				if err := batch.Commit(nil); err != nil {
					return err
				}
				batch = p.db.NewBatch()
			}
		}

		metadata.RecordCount += int64(len(records))
	}

	// Update metadata with final counts
	metadataBytes, err = p.metadataSerializer.SerializeValue(metadata)
	if err != nil {
		return fmt.Errorf("failed to serialize updated metadata: %w", err)
	}

	if err := batch.Set(metadataKey, metadataBytes, nil); err != nil {
		return err
	}

	return batch.Commit(nil)
}

func (p *Storage[K, T]) LoadWindow(ctx context.Context, windowStart time.Time) (storage.WindowState[K, T], error) {
	state := storage.WindowState[K, T]{
		WindowStart: windowStart,
		Groups:      make(map[K][]types.Record[T]),
	}

	// Load metadata
	metadataKey := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		windowStart,
		[]byte{},
	)

	metadataBytes, closer, err := p.db.Get(metadataKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return state, nil
		}
		return state, fmt.Errorf("failed to load metadata: %w", err)
	}
	closer.Close()

	metadata, err := p.metadataSerializer.DeserializeValue(metadataBytes)
	if err != nil {
		return state, fmt.Errorf("failed to deserialize metadata: %w", err)
	}

	state.WindowEnd = metadata.WindowEnd

	// Create prefix for scanning window data
	prefix := p.keySerializer.SerializeKey(
		string(WindowStateNamespace),
		windowStart,
		[]byte{},
	)

	// Temporary storage for chunks
	groupChunks := make(map[K]map[int][]types.Record[T])

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	if err != nil {
		return state, fmt.Errorf("failed to iterate over window state: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Skip if key doesn't match our prefix
		if !bytes.HasPrefix(key, prefix[:len(prefix)-1]) {
			continue
		}

		// Extract group key and chunk index
		_, _, remainingBytes, err := p.keySerializer.DeserializeKey(key)
		if err != nil {
			return state, fmt.Errorf("failed to deserialize key: %w", err)
		}

		chunkIndex := int(remainingBytes[len(remainingBytes)-1])
		groupKeyBytes := remainingBytes[:len(remainingBytes)-1]

		// Deserialize group key
		groupKey, err := p.groupKeySerializer.DeserializeValue(groupKeyBytes)
		if err != nil {
			return state, fmt.Errorf("failed to deserialize group key: %w", err)
		}

		// Deserialize chunk of records
		records, err := p.valueSerializer.DeserializeChunk(value)
		if err != nil {
			return state, fmt.Errorf("failed to deserialize chunk: %w", err)
		}

		// Initialize group chunks map if needed
		if groupChunks[groupKey] == nil {
			groupChunks[groupKey] = make(map[int][]types.Record[T])
		}

		// Store chunk
		groupChunks[groupKey][chunkIndex] = records
	}

	// Combine chunks in order for each group
	for groupKey, chunks := range groupChunks {
		var indices []int
		for idx := range chunks {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		var records []types.Record[T]
		for _, idx := range indices {
			records = append(records, chunks[idx]...)
		}

		state.Groups[groupKey] = records
	}

	return state, nil
}

func (p *Storage[K, T]) DeleteWindow(ctx context.Context, windowStart time.Time) error {
	batch := p.db.NewBatch()
	defer batch.Close()

	// Delete metadata
	metadataKey := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		windowStart,
		[]byte{},
	)
	if err := batch.Delete(metadataKey, nil); err != nil {
		return err
	}

	// Delete all records for this window
	prefix := p.keySerializer.SerializeKey(
		string(WindowStateNamespace),
		windowStart,
		[]byte{},
	)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xFF),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return err
		}

		// Commit batch if it gets too large
		if batch.Len() > 1000 {
			if err := batch.Commit(nil); err != nil {
				return err
			}
			batch = p.db.NewBatch()
		}
	}

	return batch.Commit(nil)
}

func (p *Storage[K, T]) ListWindows(ctx context.Context, start, end time.Time) ([]time.Time, error) {
	var windows []time.Time

	// Create prefix for metadata namespace to scan
	prefix := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		time.Time{}, // Zero time for prefix
		nil,
	)
	prefixLen := len(prefix) - 8 // Subtract 8 bytes for the zero timestamp

	// Create bounds for the scan
	lowerBound := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		start,
		nil,
	)
	upperBound := p.keySerializer.SerializeKey(
		string(MetadataNamespace),
		end,
		nil,
	)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound[:len(lowerBound)-1], // Remove the null terminator
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < prefixLen+8 { // prefix + timestamp
			continue
		}

		// Extract timestamp bytes
		timestampBytes := key[prefixLen : prefixLen+8]
		nanos := binary.BigEndian.Uint64(timestampBytes)
		windowTime := time.Unix(0, int64(nanos))

		// Check if window is within range
		if (windowTime.Equal(start) || windowTime.After(start)) && windowTime.Before(end) {
			windows = append(windows, windowTime)
		}
	}

	// Sort windows by time
	sort.Slice(windows, func(i, j int) bool {
		return windows[i].Before(windows[j])
	})

	return windows, nil
}

type GobSerializer[T any] struct{}

func NewGobSerializer[T any]() *GobSerializer[T] {
	// Register ChunkData type with gob
	gob.Register(ChunkData[T]{})
	return &GobSerializer[T]{}
}

func (s *GobSerializer[T]) Serialize(value interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, fmt.Errorf("gob serialization failed: %w", err)
	}
	return buf.Bytes(), nil
}

func (s *GobSerializer[T]) Deserialize(data []byte) (ChunkData[T], error) {
	var value ChunkData[T]
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return ChunkData[T]{}, fmt.Errorf("gob deserialization failed: %w", err)
	}
	return value, nil
}

// ChunkData represents a chunk of records
type ChunkData[T any] struct {
	Records []types.Record[T]
}
