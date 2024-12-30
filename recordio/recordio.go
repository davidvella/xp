package recordio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/davidvella/xp/partition"
)

var (
	Uint64Size = int64(binary.Size(uint64(0)))
	Int64Size  = int64(binary.Size(int64(0)))
	// MagicBytes Magic bytes to identify valid recordio files (REC).
	MagicBytes           = []byte{0x52, 0x45, 0x43}
	ErrInvalidMagicBytes = errors.New("invalid magic bytes - not a valid recordio file")
)

// BinaryWriter handles writing binary data with error handling.
type BinaryWriter struct {
	w io.Writer
}

func NewBinaryWriter(w io.Writer) BinaryWriter {
	return BinaryWriter{w: w}
}

func (bw BinaryWriter) WriteString(s string) (int64, error) {
	// Write string length (uint64)
	if err := binary.Write(bw.w, binary.LittleEndian, uint64(len(s))); err != nil {
		return 0, fmt.Errorf("error writing string length: %w", err)
	}

	// Write string content
	n, err := bw.w.Write([]byte(s))
	if err != nil {
		return Uint64Size, fmt.Errorf("error writing string content: %w", err)
	}

	// Return total bytes written (length field + string content)
	return Uint64Size + int64(n), nil
}

func (bw BinaryWriter) WriteInt64(i int64) (int64, error) {
	err := binary.Write(bw.w, binary.LittleEndian, i)
	if err != nil {
		return 0, err
	}
	return Int64Size, nil
}

func (bw BinaryWriter) WriteBytes(b []byte) (int64, error) {
	// Write bytes length (uint64)
	if err := binary.Write(bw.w, binary.LittleEndian, uint64(len(b))); err != nil {
		return 0, fmt.Errorf("error writing bytes length: %w", err)
	}

	// Write bytes content
	n, err := bw.w.Write(b)
	if err != nil {
		return Uint64Size, fmt.Errorf("error writing bytes content: %w", err)
	}

	// Return total bytes written (length field + bytes content)
	return Uint64Size + int64(n), nil
}

// BinaryReader handles reading binary data with error handling.
type BinaryReader struct {
	r io.Reader
}

func NewBinaryReader(r io.Reader) BinaryReader {
	return BinaryReader{r: r}
}

func (br BinaryReader) ReadString() (string, error) {
	var length uint64
	if err := binary.Read(br.r, binary.LittleEndian, &length); err != nil {
		return "", fmt.Errorf("error reading string length: %w", err)
	}

	b := make([]byte, length)
	if _, err := io.ReadFull(br.r, b); err != nil {
		return "", fmt.Errorf("error reading string content: %w", err)
	}
	return string(b), nil
}

func (br BinaryReader) ReadInt64() (int64, error) {
	var value int64
	err := binary.Read(br.r, binary.LittleEndian, &value)
	return value, err
}

func (br BinaryReader) ReadBytes() ([]byte, error) {
	var length uint64
	if err := binary.Read(br.r, binary.LittleEndian, &length); err != nil {
		return nil, fmt.Errorf("error reading bytes length: %w", err)
	}

	bytes := make([]byte, length)
	if _, err := io.ReadFull(br.r, bytes); err != nil {
		return nil, fmt.Errorf("error reading bytes content: %w", err)
	}
	return bytes, nil
}

// Write writes a single record to the writer.
func Write(w io.Writer, data partition.Record) (int64, error) {
	if data == nil {
		return 0, nil
	}

	var (
		totalBytes int64
		n          int64
	)

	mn, err := w.Write(MagicBytes)
	if err != nil {
		return int64(mn), fmt.Errorf("failed to write magic bytes: %w", err)
	}
	totalBytes += int64(mn)

	bw := NewBinaryWriter(w)

	n, err = bw.WriteString(data.GetID())
	if err != nil {
		return totalBytes, fmt.Errorf("error writing ID: %w", err)
	}
	totalBytes += n

	n, err = bw.WriteString(data.GetPartitionKey())
	if err != nil {
		return totalBytes, fmt.Errorf("error writing partition key: %w", err)
	}
	totalBytes += n

	n, err = bw.WriteInt64(data.GetWatermark().UnixNano())
	if err != nil {
		return totalBytes, fmt.Errorf("error writing timestamp: %w", err)
	}
	totalBytes += n

	n, err = bw.WriteString(data.GetWatermark().Location().String())
	if err != nil {
		return totalBytes, fmt.Errorf("error writing timezone: %w", err)
	}
	totalBytes += n

	n, err = bw.WriteBytes(data.GetData())
	if err != nil {
		return totalBytes, fmt.Errorf("error writing data: %w", err)
	}
	totalBytes += n

	return totalBytes, nil
}

// ReadRecord reads a single record from the reader.
func ReadRecord(r io.Reader) (partition.Record, error) {
	magicBytes := make([]byte, len(MagicBytes))
	if _, err := io.ReadFull(r, magicBytes); err != nil {
		return nil, fmt.Errorf("failed to read magic bytes: %w", err)
	}
	if !bytes.Equal(magicBytes, MagicBytes) {
		return nil, ErrInvalidMagicBytes
	}

	br := NewBinaryReader(r)

	id, err := br.ReadString()
	if err != nil {
		return nil, fmt.Errorf("error reading ID: %w", err)
	}

	partitionKey, err := br.ReadString()
	if err != nil {
		return nil, fmt.Errorf("error reading partition key: %w", err)
	}

	unixNano, err := br.ReadInt64()
	if err != nil {
		return nil, fmt.Errorf("error reading timestamp: %w", err)
	}

	timezone, err := br.ReadString()
	if err != nil {
		return nil, fmt.Errorf("error reading timezone: %w", err)
	}

	//nolint:errcheck // Can't set an invalid timezone
	loc, _ := time.LoadLocation(timezone)

	timestamp := time.Unix(0, unixNano).In(loc)

	data, err := br.ReadBytes()
	if err != nil {
		return nil, fmt.Errorf("error reading data: %w", err)
	}

	return partition.RecordImpl{
		ID:           id,
		PartitionKey: partitionKey,
		Timestamp:    timestamp,
		Data:         data,
	}, nil
}

// Seq creates an iterator over records.
func Seq(r io.Reader) iter.Seq[partition.Record] {
	return func(yield func(partition.Record) bool) {
		for {
			record, err := ReadRecord(r)
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				return
			}
			if !yield(record) {
				return
			}
		}
	}
}

// ReadRecords reads all records into a slice.
func ReadRecords(r io.Reader) []partition.Record {
	records := make([]partition.Record, 0, 1)
	for record := range Seq(r) {
		records = append(records, record)
	}
	return records
}

// Size calculates the total size in bytes that a record will occupy when written.
// This includes magic bytes, all fields and their length prefixes.
func Size(record partition.Record) int64 {
	if record == nil {
		return 0
	}

	var totalSize int64

	// Magic bytes size
	totalSize += int64(len(MagicBytes))

	// ID field: length prefix + content
	totalSize += Uint64Size + int64(len(record.GetID()))

	// PartitionKey field: length prefix + content
	totalSize += Uint64Size + int64(len(record.GetPartitionKey()))

	// Timestamp: int64 for UnixNano
	totalSize += Int64Size

	// Timezone: length prefix + content
	timezone := record.GetWatermark().Location().String()
	totalSize += Uint64Size + int64(len(timezone))

	// Data field: length prefix + content
	totalSize += Uint64Size + int64(len(record.GetData()))

	return totalSize
}
