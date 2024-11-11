package pebble

import "time"

// StateNamespace identifies different types of state
type StateNamespace string

const (
	WindowStateNamespace  StateNamespace = "window"
	TriggerStateNamespace StateNamespace = "trigger"
	MetadataNamespace     StateNamespace = "metadata"
)

// WindowMetadata stores window configuration and stats
type WindowMetadata struct {
	WindowStart    time.Time
	WindowEnd      time.Time
	LastUpdateTime time.Time
	RecordCount    int64
	SizeBytes      int64
}
