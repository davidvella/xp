package recordio

import (
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/davidvella/xp/core/partition"
)

// Write writes a single record to the writer
func Write(w io.Writer, data partition.Record) error {
	// Handle ID length and ID
	if err := writeString(w, data.GetID()); err != nil {
		return err
	}

	// Handle PartitionKey length and PartitionKey
	if err := writeString(w, data.GetPartitionKey()); err != nil {
		return err
	}

	// Handle timestamp (8 bytes)
	if err := binary.Write(w, binary.LittleEndian, data.GetWatermark().UnixNano()); err != nil {
		return fmt.Errorf("error writing timestamp: %v", err)
	}

	// Handle timezone name
	if err := writeString(w, data.GetWatermark().Location().String()); err != nil {
		return fmt.Errorf("error writing timezone: %v", err)
	}

	// Handle data length and data
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data.GetData()))); err != nil {
		return fmt.Errorf("error writing data length: %v", err)
	}
	if _, err := w.Write(data.GetData()); err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}

	// Handle new line (1 byte)
	if _, err := w.Write([]byte{'\n'}); err != nil {
		return fmt.Errorf("error writing newline: %v", err)
	}

	return nil
}

func writeString(w io.Writer, data string) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return fmt.Errorf("error writing: %v", err)
	}
	if _, err := w.Write([]byte(data)); err != nil {
		return fmt.Errorf("error writing: %v", err)
	}
	return nil
}

// ReadRecord reads a single record from the reader
func ReadRecord(r io.Reader) (partition.Record, error) {
	// Read ID length
	var idLen uint32
	if err := binary.Read(r, binary.LittleEndian, &idLen); err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("error reading ID length: %v", err)
	}

	// Read ID
	idBytes := make([]byte, idLen)
	if _, err := io.ReadFull(r, idBytes); err != nil {
		return nil, fmt.Errorf("error reading ID: %v", err)
	}

	// Read PartitionKey length
	var partitionKeyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &partitionKeyLen); err != nil {
		return nil, fmt.Errorf("error reading PartitionKey length: %v", err)
	}

	// Read PartitionKey
	partitionKeyBytes := make([]byte, partitionKeyLen)
	if _, err := io.ReadFull(r, partitionKeyBytes); err != nil {
		return nil, fmt.Errorf("error reading PartitionKey: %v", err)
	}

	// Read timestamp
	var unixNano int64
	if err := binary.Read(r, binary.LittleEndian, &unixNano); err != nil {
		return nil, fmt.Errorf("error reading timestamp: %v", err)
	}

	// Read timezone length and name
	var timezoneLen uint32
	if err := binary.Read(r, binary.LittleEndian, &timezoneLen); err != nil {
		return nil, fmt.Errorf("error reading timezone length: %v", err)
	}

	timezoneBytes := make([]byte, timezoneLen)
	if _, err := io.ReadFull(r, timezoneBytes); err != nil {
		return nil, fmt.Errorf("error reading timezone: %v", err)
	}

	// Load the timezone
	loc, err := time.LoadLocation(string(timezoneBytes))
	if err != nil {
		return nil, fmt.Errorf("error loading timezone: %v", err)
	}

	// Read data length
	var dataLen uint32
	if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
		return nil, fmt.Errorf("error reading data length: %v", err)
	}

	// Read data
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, fmt.Errorf("error reading data: %v", err)
	}

	// Read newline
	nl := make([]byte, 1)
	if _, err := io.ReadFull(r, nl); err != nil {
		return nil, fmt.Errorf("error reading newline: %v", err)
	}

	// Create timestamp with the correct timezone
	timestamp := time.Unix(0, unixNano).In(loc)

	return partition.RecordImpl{
		ID:           string(idBytes),
		PartitionKey: string(partitionKeyBytes),
		Timestamp:    timestamp,
		Data:         data,
	}, nil
}

func Seq(r io.Reader) iter.Seq[partition.Record] {
	return func(yield func(partition.Record) bool) {
		for {
			record, err := ReadRecord(r)
			if err == io.EOF {
				return
			}
			if err != nil {
				// You might want to handle errors differently
				return
			}
			if !yield(record) {
				return
			}
		}
	}
}

func ReadRecords(r io.Reader) []partition.Record {
	var records []partition.Record
	for record := range Seq(r) {
		records = append(records, record)
	}
	return records
}
