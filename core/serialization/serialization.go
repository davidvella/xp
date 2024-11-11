package serialization

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"time"

	"github.com/davidvella/xp/core/types"
)

// TypeSerializer defines how to serialize/deserialize specific types
type TypeSerializer[T any] interface {
	SerializeValue(value T) ([]byte, error)
	DeserializeValue(data []byte) (T, error)
	SerializeChunk(records []types.Record[T]) ([]byte, error)
	DeserializeChunk(data []byte) ([]types.Record[T], error)
}

// KeySerializer handles composite key serialization
type KeySerializer struct{}

func (ks *KeySerializer) SerializeKey(namespace string, windowStart time.Time, key []byte) []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(namespace)
	buf.WriteByte(0) // namespace separator
	binary.Write(buf, binary.BigEndian, windowStart.UnixNano())
	buf.Write(key)
	return buf.Bytes()
}

func (ks *KeySerializer) DeserializeKey(key []byte) (string, time.Time, []byte, error) {
	parts := bytes.SplitN(key, []byte{0}, 2)
	if len(parts) != 2 {
		return "", time.Time{}, nil, nil
	}

	namespace := string(parts[0])
	remainder := parts[1]

	timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(remainder[:8])))
	key = remainder[8:]

	return namespace, timestamp, key, nil
}

// GobSerializer implements TypeSerializer using Gob encoding
type GobSerializer[T any] struct{}

func NewGobSerializer[T any]() *GobSerializer[T] {
	// Register types with gob
	gob.Register([]types.Record[T]{})
	return &GobSerializer[T]{}
}

func (s *GobSerializer[T]) SerializeValue(value T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *GobSerializer[T]) DeserializeValue(data []byte) (T, error) {
	var value T
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&value); err != nil {
		return value, err
	}
	return value, nil
}

func (s *GobSerializer[T]) SerializeChunk(records []types.Record[T]) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(records); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *GobSerializer[T]) DeserializeChunk(data []byte) ([]types.Record[T], error) {
	var records []types.Record[T]
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&records); err != nil {
		return nil, err
	}
	return records, nil
}
