package monitoring

import (
	"context"
	"encoding/json"
	"os"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Component string                 `json:"component"`
	EventType string                 `json:"event_type"`
	Details   map[string]interface{} `json:"details,omitempty"`
}

type logger struct {
	component string
}

func NewLogger(component string) *logger {
	return &logger{
		component: component,
	}
}

func (l *logger) Log(ctx context.Context, level LogLevel, eventType string, message string, details map[string]interface{}) {
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level.String(),
		Message:   message,
		Component: l.component,
		EventType: eventType,
		Details:   details,
	}

	json.NewEncoder(os.Stdout).Encode(entry)
}

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

type Logger interface {
	Log(ctx context.Context, level LogLevel, eventType string, message string, details map[string]interface{})
}
