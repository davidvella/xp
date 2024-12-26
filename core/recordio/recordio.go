package recordio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/davidvella/xp/core/partition"
)

// BinaryWriter handles writing binary data with error handling.
type BinaryWriter struct {
	w io.Writer
}

func NewBinaryWriter(w io.Writer) BinaryWriter {
	return BinaryWriter{w: w}
}

func (bw BinaryWriter) WriteString(s string) error {
	if err := binary.Write(bw.w, binary.LittleEndian, uint64(len(s))); err != nil {
		return fmt.Errorf("error writing string length: %w", err)
	}
	if _, err := bw.w.Write([]byte(s)); err != nil {
		return fmt.Errorf("error writing string content: %w", err)
	}
	return nil
}

func (bw BinaryWriter) WriteInt64(i int64) error {
	return binary.Write(bw.w, binary.LittleEndian, i)
}

func (bw BinaryWriter) WriteBytes(b []byte) error {
	if err := binary.Write(bw.w, binary.LittleEndian, uint64(len(b))); err != nil {
		return fmt.Errorf("error writing bytes length: %w", err)
	}
	if _, err := bw.w.Write(b); err != nil {
		return fmt.Errorf("error writing bytes content: %w", err)
	}
	return nil
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

	bytes := make([]byte, length)
	if _, err := io.ReadFull(br.r, bytes); err != nil {
		return "", fmt.Errorf("error reading string content: %w", err)
	}
	return string(bytes), nil
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
func Write(w io.Writer, data partition.Record) error {
	if data == nil {
		return nil
	}

	bw := NewBinaryWriter(w)

	if err := bw.WriteString(data.GetID()); err != nil {
		return fmt.Errorf("error writing ID: %w", err)
	}

	if err := bw.WriteString(data.GetPartitionKey()); err != nil {
		return fmt.Errorf("error writing partition key: %w", err)
	}

	if err := bw.WriteInt64(data.GetWatermark().UnixNano()); err != nil {
		return fmt.Errorf("error writing timestamp: %w", err)
	}

	if err := bw.WriteString(data.GetWatermark().Location().String()); err != nil {
		return fmt.Errorf("error writing timezone: %w", err)
	}

	if err := bw.WriteBytes(data.GetData()); err != nil {
		return fmt.Errorf("error writing data: %w", err)
	}

	if _, err := w.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("error writing newline: %w", err)
	}

	return nil
}

// ReadRecord reads a single record from the reader.
func ReadRecord(r io.Reader) (partition.Record, error) {
	br := NewBinaryReader(r)

	id, err := br.ReadString()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, err
		}
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

	loc, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, fmt.Errorf("error loading timezone: %w", err)
	}

	timestamp := time.Unix(0, unixNano).In(loc)

	data, err := br.ReadBytes()
	if err != nil {
		return nil, fmt.Errorf("error reading data: %w", err)
	}

	nl := make([]byte, 1)
	if _, err := io.ReadFull(r, nl); err != nil {
		return nil, fmt.Errorf("error reading newline: %w", err)
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
