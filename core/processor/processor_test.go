package processor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/davidvella/xp/core/monitoring"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/triggers"
	"github.com/davidvella/xp/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvent represents test data
type TestEvent struct {
	ID     string
	Value  int64
	Region string
}

// Test implementations
type testStrategy struct {
	windows  []time.Time
	complete bool
}

func (s *testStrategy) AssignWindows(record types.Record[TestEvent]) []time.Time {
	return s.windows
}

func (s *testStrategy) IsWindowComplete(window, watermark time.Time) bool {
	return s.complete
}

type testGroupFunction struct {
	key string
}

func (f *testGroupFunction) GetKey(record types.Record[TestEvent]) string {
	return f.key
}

type testAggregator struct {
	sum int64
}

func (a *testAggregator) CreateAccumulator() int64 {
	return 0
}

func (a *testAggregator) AddInput(acc int64, input types.Record[TestEvent]) int64 {
	return acc + input.Data.Value
}

func (a *testAggregator) MergeAccumulators(acc1, acc2 int64) int64 {
	return acc1 + acc2
}

func (a *testAggregator) GetResult(acc int64) int64 {
	return acc
}

type testTrigger struct {
	shouldFire bool
	cleared    map[time.Time]bool
}

func newTestTrigger(shouldFire bool) *testTrigger {
	return &testTrigger{
		cleared:    make(map[time.Time]bool),
		shouldFire: shouldFire,
	}
}

func (t *testTrigger) OnElement(ctx triggers.TriggerContext[TestEvent]) bool {
	return t.shouldFire
}

func (t *testTrigger) OnProcessingTime(ctx triggers.TriggerContext[TestEvent]) bool {
	return t.shouldFire
}

func (t *testTrigger) OnEventTime(ctx triggers.TriggerContext[TestEvent]) bool {
	return t.shouldFire
}

func (t *testTrigger) Clear(windowTime time.Time) {
	t.cleared[windowTime] = true
}

// InMemoryStorage for testing
type InMemoryStorage struct {
	windows map[time.Time]storage.WindowState[string, TestEvent]
	mu      sync.RWMutex
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		windows: make(map[time.Time]storage.WindowState[string, TestEvent]),
	}
}

func (s *InMemoryStorage) SaveWindow(ctx context.Context, state storage.WindowState[string, TestEvent]) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.windows[state.WindowStart] = state
	return nil
}

func (s *InMemoryStorage) LoadWindow(ctx context.Context, windowStart time.Time) (storage.WindowState[string, TestEvent], error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.windows[windowStart]
	if !ok {
		return storage.WindowState[string, TestEvent]{}, nil
	}
	return state, nil
}

func (s *InMemoryStorage) DeleteWindow(ctx context.Context, windowStart time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.windows, windowStart)
	return nil
}

func (s *InMemoryStorage) ListWindows(ctx context.Context, start, end time.Time) ([]time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var windows []time.Time
	for t := range s.windows {
		if (t.Equal(start) || t.After(start)) && t.Before(end) {
			windows = append(windows, t)
		}
	}
	return windows, nil
}

// MockStats records metric operations for testing
type MockStats struct {
	mu sync.RWMutex

	// Counters
	ProcessedRecords int
	WindowsEmitted   int
	ErrorsRecorded   int

	// Latencies
	ProcessingLatencies []time.Duration

	// Labels recorded with each operation
	RecordLabels  []map[string]string
	WindowLabels  []map[string]string
	ErrorLabels   []map[string]string
	LatencyLabels []map[string]string

	// Active windows
	ActiveWindows int

	// Last recorded values for verification
	LastRecordContext context.Context
	LastErrorType     string
}

func NewMockStats() *MockStats {
	return &MockStats{
		RecordLabels:  make([]map[string]string, 0),
		WindowLabels:  make([]map[string]string, 0),
		ErrorLabels:   make([]map[string]string, 0),
		LatencyLabels: make([]map[string]string, 0),
	}
}

func (m *MockStats) RecordProcessedRecord(ctx context.Context, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessedRecords++
	m.RecordLabels = append(m.RecordLabels, labels)
	m.LastRecordContext = ctx
}

func (m *MockStats) RecordWindowEmitted(ctx context.Context, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.WindowsEmitted++
	m.WindowLabels = append(m.WindowLabels, labels)
}

func (m *MockStats) RecordError(ctx context.Context, errorType string, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ErrorsRecorded++
	m.ErrorLabels = append(m.ErrorLabels, labels)
	m.LastErrorType = errorType
}

func (m *MockStats) RecordProcessingLatency(ctx context.Context, duration time.Duration, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ProcessingLatencies = append(m.ProcessingLatencies, duration)
	m.LatencyLabels = append(m.LatencyLabels, labels)
}

func (m *MockStats) RecordProcessError(ctx context.Context, err string) {
	//TODO implement me
	panic("implement me")
}

func (m *MockStats) SetActiveWindows(ctx context.Context, count int, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveWindows = count
}

// Helper methods for assertions
func (m *MockStats) GetProcessedRecords() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ProcessedRecords
}

func (m *MockStats) GetWindowsEmitted() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.WindowsEmitted
}

func (m *MockStats) GetErrorsRecorded() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ErrorsRecorded
}

func (m *MockStats) GetLastErrorType() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.LastErrorType
}

func (m *MockStats) GetProcessingLatencies() []time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]time.Duration{}, m.ProcessingLatencies...)
}

func (m *MockStats) GetActiveWindows() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ActiveWindows
}

// MockLogger records log operations for testing
type MockLogger struct {
	mu sync.RWMutex

	// Log entries
	Entries []LogEntry

	// Filters for testing specific scenarios
	ErrorOnly bool
}

type LogEntry struct {
	Level     monitoring.LogLevel
	EventType string
	Message   string
	Details   map[string]interface{}
	Time      time.Time
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		Entries: make([]LogEntry, 0),
	}
}

func (m *MockLogger) Log(ctx context.Context, level monitoring.LogLevel, eventType string, message string, details map[string]interface{}) {
	if m.ErrorOnly && level != monitoring.ERROR {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.Entries = append(m.Entries, LogEntry{
		Level:     level,
		EventType: eventType,
		Message:   message,
		Details:   details,
		Time:      time.Now(),
	})
}

// Helper methods for assertions
func (m *MockLogger) GetEntries() []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]LogEntry{}, m.Entries...)
}

func (m *MockLogger) GetEntriesByLevel(level monitoring.LogLevel) []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var filtered []LogEntry
	for _, entry := range m.Entries {
		if entry.Level == level {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (m *MockLogger) GetEntriesByEventType(eventType string) []LogEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var filtered []LogEntry
	for _, entry := range m.Entries {
		if entry.EventType == eventType {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (m *MockLogger) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Entries = make([]LogEntry, 0)
}

// Tests
func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name           string
		windows        []time.Time
		triggerFires   bool
		expectedResult int64
		expectedError  error
	}{
		{
			name:           "single window, trigger fires",
			windows:        []time.Time{time.Now()},
			triggerFires:   true,
			expectedResult: 100,
			expectedError:  nil,
		},
		{
			name:           "single window, no trigger",
			windows:        []time.Time{time.Now()},
			triggerFires:   false,
			expectedResult: 0,
			expectedError:  nil,
		},
		{
			name:           "multiple windows",
			windows:        []time.Time{time.Now(), time.Now().Add(time.Hour)},
			triggerFires:   true,
			expectedResult: 100,
			expectedError:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup components
			strategy := &testStrategy{windows: tt.windows}
			groupFn := &testGroupFunction{key: "test-key"}
			aggregator := &testAggregator{}
			trigger := newTestTrigger(tt.triggerFires)
			stats := NewMockStats()
			logger := NewMockLogger()
			s := NewInMemoryStorage()
			processorConfig := ProcessorConfig{
				WatermarkInterval: 10 * time.Millisecond,
				CleanupInterval:   10 * time.Millisecond,
			}

			// Create processor
			proc := NewProcessor[string, TestEvent, int64](
				strategy,
				groupFn,
				aggregator,
				trigger,
				s,
				stats,
				logger,
				processorConfig,
			)

			// Start processor
			err := proc.Start()
			require.NoError(t, err)
			defer proc.Stop()

			// Process test event
			ctx := context.Background()
			event := TestEvent{
				ID:     "test",
				Value:  100,
				Region: "test-region",
			}
			record := types.Record[TestEvent]{
				Data:      event,
				Timestamp: time.Now(),
			}

			// Process record
			err = proc.Process(ctx, record)
			if tt.expectedError != nil {
				assert.Equal(t, tt.expectedError, err)
				return
			}
			require.NoError(t, err)

			// If trigger fired, check results
			if tt.triggerFires {
				select {
				case result := <-proc.Output():
					assert.Equal(t, tt.expectedResult, result.Results[groupFn.key])
					assert.Eventually(t, func() bool {
						return assert.Len(t, trigger.cleared, len(tt.windows))
					}, time.Second, time.Millisecond*10)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for results")
				}
			} else {
				// Verify data was stored
				for _, window := range tt.windows {
					state, err := s.LoadWindow(ctx, window)
					require.NoError(t, err)
					assert.Len(t, state.Groups[groupFn.key], 1)
					assert.Equal(t, event, state.Groups[groupFn.key][0].Data)
				}
			}
		})
	}
}

func TestProcessor_Watermark(t *testing.T) {
	t.Run("watermark triggers window emission", func(t *testing.T) {
		// Setup
		now := time.Now()
		strategy := &testStrategy{
			windows:  []time.Time{now},
			complete: true,
		}
		storage := NewInMemoryStorage()
		trigger := newTestTrigger(false)

		proc := NewProcessor[string, TestEvent, int64](
			strategy,
			&testGroupFunction{key: "test"},
			&testAggregator{},
			trigger,
			storage,
			nil,
			nil,
			ProcessorConfig{
				WatermarkInterval: 100 * time.Millisecond,
			},
		)

		// Start processor
		err := proc.Start()
		require.NoError(t, err)
		defer proc.Stop()

		// Process event
		ctx := context.Background()
		err = proc.Process(ctx, types.Record[TestEvent]{
			Data:      TestEvent{Value: 100},
			Timestamp: now,
		})
		require.NoError(t, err)

		// Wait for watermark to advance
		time.Sleep(200 * time.Millisecond)

		// Verify window was cleared
		assert.True(t, trigger.cleared[now])
	})
}

func TestProcessor_Lifecycle(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*testing.T, *Processor[string, TestEvent, int64])
	}{
		{
			name: "start twice",
			fn: func(t *testing.T, p *Processor[string, TestEvent, int64]) {
				err := p.Start()
				require.NoError(t, err)
				err = p.Start()
				assert.Equal(t, ErrProcessorClosed, err)
			},
		},
		{
			name: "stop twice",
			fn: func(t *testing.T, p *Processor[string, TestEvent, int64]) {
				err := p.Start()
				require.NoError(t, err)
				err = p.Stop()
				require.NoError(t, err)
				err = p.Stop()
				assert.Equal(t, ErrProcessorClosed, err)
			},
		},
		{
			name: "process after stop",
			fn: func(t *testing.T, p *Processor[string, TestEvent, int64]) {
				err := p.Start()
				require.NoError(t, err)
				err = p.Stop()
				require.NoError(t, err)
				err = p.Process(context.Background(), types.Record[TestEvent]{})
				assert.Equal(t, ErrProcessorClosed, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := NewProcessor[string, TestEvent, int64](
				&testStrategy{},
				&testGroupFunction{},
				&testAggregator{},
				newTestTrigger(false),
				NewInMemoryStorage(),
				nil,
				nil,
				ProcessorConfig{},
			)
			tt.fn(t, proc)
		})
	}
}

func BenchmarkProcessor_Process(b *testing.B) {
	// Setup processor
	proc := NewProcessor[string, TestEvent, int64](
		&testStrategy{windows: []time.Time{time.Now()}},
		&testGroupFunction{key: "test"},
		&testAggregator{},
		newTestTrigger(false),
		NewInMemoryStorage(),
		nil, nil,
		ProcessorConfig{},
	)

	err := proc.Start()
	require.NoError(b, err)
	defer proc.Stop()

	ctx := context.Background()
	record := types.Record[TestEvent]{
		Data:      TestEvent{Value: 100},
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := proc.Process(ctx, record)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestProcessor_Cleanup(t *testing.T) {
	t.Run("old windows are cleaned up", func(t *testing.T) {
		s := NewInMemoryStorage()
		proc := NewProcessor[string, TestEvent, int64](
			&testStrategy{},
			&testGroupFunction{},
			&testAggregator{},
			newTestTrigger(false),
			s,
			nil,
			nil,
			ProcessorConfig{
				CleanupInterval: 100 * time.Millisecond,
			},
		)

		// Create old window state
		oldWindow := time.Now().Add(-25 * time.Hour)
		ctx := context.Background()
		err := s.SaveWindow(ctx, storage.WindowState[string, TestEvent]{
			WindowStart: oldWindow,
		})
		require.NoError(t, err)

		// Start processor
		err = proc.Start()
		require.NoError(t, err)
		defer proc.Stop()

		// Wait for cleanup
		time.Sleep(200 * time.Millisecond)

		// Verify old window was cleaned up
		windows, err := s.ListWindows(ctx, time.Time{}, time.Now())
		require.NoError(t, err)
		assert.Empty(t, windows)
	})
}
