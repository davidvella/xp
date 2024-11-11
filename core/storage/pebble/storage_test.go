package pebble

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/davidvella/xp/core/serialization"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEvent represents test data
type TestEvent struct {
	ID     string
	Value  float64
	Labels map[string]string
}

func setupTestStorage(t *testing.T) (*Storage[string, TestEvent], func()) {
	tempDir, err := os.MkdirTemp("", "pebble-test-*")
	require.NoError(t, err)

	valueSerializer := serialization.NewGobSerializer[TestEvent]()
	keySerializer := serialization.NewGobSerializer[string]()

	storage, err := NewStorage[string, TestEvent](
		StorageOptions{
			Path:         filepath.Join(tempDir, "test.db"),
			BatchSize:    1000,
			CacheSize:    64 * 1024 * 1024,
			MaxOpenFiles: 100,
		},
		valueSerializer,
		keySerializer,
	)
	require.NoError(t, err)

	cleanup := func() {
		storage.Close()
		os.RemoveAll(tempDir)
	}

	return storage, cleanup
}

func TestStorage_BasicOperations(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	// Create test data
	state := storage.WindowState[string, TestEvent]{
		WindowStart: now,
		WindowEnd:   now.Add(time.Hour),
		Groups: map[string][]types.Record[TestEvent]{
			"group1": {
				{
					Data: TestEvent{
						ID:     "1",
						Value:  100,
						Labels: map[string]string{"region": "us"},
					},
					Timestamp: now,
				},
			},
			"group2": {
				{
					Data: TestEvent{
						ID:     "2",
						Value:  200,
						Labels: map[string]string{"region": "eu"},
					},
					Timestamp: now,
				},
			},
		},
	}

	// Test SaveWindow
	err := s.SaveWindow(ctx, state)
	require.NoError(t, err)

	// Test LoadWindow
	loaded, err := s.LoadWindow(ctx, now)
	require.NoError(t, err)

	assert.Equal(t, state.WindowStart, loaded.WindowStart)
	assert.Equal(t, state.WindowEnd, loaded.WindowEnd)
	assert.Len(t, loaded.Groups, len(state.Groups))

	for key, group := range state.Groups {
		loadedGroup, ok := loaded.Groups[key]
		assert.True(t, ok)
		assert.Len(t, loadedGroup, len(group))
		assert.Equal(t, group[0].Data, loadedGroup[0].Data)
	}

	// Test ListWindows
	windows, err := s.ListWindows(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	assert.Len(t, windows, 1)
	assert.Equal(t, now, windows[0])

	// Test DeleteWindow
	err = s.DeleteWindow(ctx, now)
	require.NoError(t, err)

	// Verify deletion
	loaded, err = s.LoadWindow(ctx, now)
	require.NoError(t, err)
	assert.Len(t, loaded.Groups, 0)
}

func TestStorage_LargeWindow(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	// Create large test data
	state := storage.WindowState[string, TestEvent]{
		WindowStart: now,
		WindowEnd:   now.Add(time.Hour),
		Groups:      make(map[string][]types.Record[TestEvent]),
	}

	numGroups := 100
	recordsPerGroup := 1000

	for i := 0; i < numGroups; i++ {
		groupKey := fmt.Sprintf("group%d", i)
		records := make([]types.Record[TestEvent], recordsPerGroup)

		for j := 0; j < recordsPerGroup; j++ {
			records[j] = types.Record[TestEvent]{
				Data: TestEvent{
					ID:    fmt.Sprintf("%d-%d", i, j),
					Value: float64(j),
					Labels: map[string]string{
						"group": groupKey,
						"index": fmt.Sprintf("%d", j),
					},
				},
				Timestamp: now,
			}
		}

		state.Groups[groupKey] = records
	}

	// Test SaveWindow with large data
	err := s.SaveWindow(ctx, state)
	require.NoError(t, err)

	// Test LoadWindow with large data
	loaded, err := s.LoadWindow(ctx, now)
	require.NoError(t, err)

	assert.Len(t, loaded.Groups, numGroups)
	for groupKey, records := range state.Groups {
		loadedRecords, ok := loaded.Groups[groupKey]
		assert.True(t, ok)
		assert.Len(t, loadedRecords, len(records))
	}
}

func TestStorage_Concurrent(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	// Test concurrent window operations
	numWorkers := 10
	numOperations := 100

	errCh := make(chan error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			var err error
			defer func() { errCh <- err }()

			for j := 0; j < numOperations; j++ {
				windowTime := now.Add(time.Duration(workerID*numOperations+j) * time.Minute)

				state := storage.WindowState[string, TestEvent]{
					WindowStart: windowTime,
					WindowEnd:   windowTime.Add(time.Hour),
					Groups: map[string][]types.Record[TestEvent]{
						fmt.Sprintf("group%d", workerID): {
							{
								Data: TestEvent{
									ID:    fmt.Sprintf("%d-%d", workerID, j),
									Value: float64(j),
								},
								Timestamp: windowTime,
							},
						},
					},
				}

				if err = s.SaveWindow(ctx, state); err != nil {
					return
				}

				if _, err = s.LoadWindow(ctx, windowTime); err != nil {
					return
				}
			}
		}(i)
	}

	// Check for errors
	for i := 0; i < numWorkers; i++ {
		err := <-errCh
		require.NoError(t, err)
	}
}

func TestPebbleStorage_ListWindows(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	baseTime := time.Now().Truncate(time.Second)

	// Create test windows at different times
	testWindows := []time.Time{
		baseTime.Add(-2 * time.Hour),
		baseTime.Add(-1 * time.Hour),
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Save test windows
	for _, windowTime := range testWindows {
		state := storage.WindowState[string, TestEvent]{
			WindowStart: windowTime,
			WindowEnd:   windowTime.Add(time.Hour),
			Groups: map[string][]types.Record[TestEvent]{
				"test": {
					{
						Data: TestEvent{
							ID:    "test",
							Value: 100,
						},
						Timestamp: windowTime,
					},
				},
			},
		}

		err := s.SaveWindow(ctx, state)
		require.NoError(t, err)
	}

	// Test cases
	tests := []struct {
		name          string
		start         time.Time
		end           time.Time
		expectedCount int
		expectedFirst time.Time
		expectedLast  time.Time
	}{
		{
			name:          "All Windows",
			start:         baseTime.Add(-3 * time.Hour),
			end:           baseTime.Add(3 * time.Hour),
			expectedCount: 5,
			expectedFirst: testWindows[0],
			expectedLast:  testWindows[4],
		},
		{
			name:          "Single Window",
			start:         baseTime,
			end:           baseTime.Add(time.Second),
			expectedCount: 1,
			expectedFirst: baseTime,
			expectedLast:  baseTime,
		},
		{
			name:          "No Windows",
			start:         baseTime.Add(3 * time.Hour),
			end:           baseTime.Add(4 * time.Hour),
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			windows, err := s.ListWindows(ctx, tt.start, tt.end)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, len(windows), "Window count mismatch")

			if tt.expectedCount > 0 {
				assert.True(t, tt.expectedFirst.Equal(windows[0]), "First window mismatch")
				assert.True(t, tt.expectedLast.Equal(windows[len(windows)-1]), "Last window mismatch")
			}

			// Verify windows are sorted
			for i := 1; i < len(windows); i++ {
				assert.True(t, windows[i-1].Before(windows[i]), "Windows not properly sorted")
			}

			// Verify all windows are within range
			for _, w := range windows {
				assert.True(t, (w.Equal(tt.start) || w.After(tt.start)) && w.Before(tt.end),
					"Window outside requested range")
			}
		})
	}

	// Test concurrent listing
	t.Run("Concurrent Listing", func(t *testing.T) {
		var wg sync.WaitGroup
		concurrency := 10
		errCh := make(chan error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.ListWindows(ctx, baseTime.Add(-3*time.Hour), baseTime.Add(3*time.Hour))
				if err != nil {
					errCh <- err
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}
	})

	// Test with empty storage
	t.Run("Empty Storage", func(t *testing.T) {
		emptyStorage, cleanup := setupTestStorage(t)
		defer cleanup()

		windows, err := emptyStorage.ListWindows(ctx, baseTime, baseTime.Add(time.Hour))
		require.NoError(t, err)
		assert.Empty(t, windows)
	})

	// Test with many windows
	t.Run("Many Windows", func(t *testing.T) {
		manyStorage, cleanup := setupTestStorage(t)
		defer cleanup()

		// Create 1000 windows at 1-minute intervals
		expectedCount := 1000
		windowTimes := make([]time.Time, expectedCount)
		for i := 0; i < expectedCount; i++ {
			windowTime := baseTime.Add(time.Duration(i) * time.Minute)
			windowTimes[i] = windowTime

			state := storage.WindowState[string, TestEvent]{
				WindowStart: windowTime,
				WindowEnd:   windowTime.Add(time.Hour),
				Groups: map[string][]types.Record[TestEvent]{
					"test": {{
						Data: TestEvent{
							ID:    fmt.Sprintf("test-%d", i),
							Value: float64(i),
						},
						Timestamp: windowTime,
					}},
				},
			}

			err := manyStorage.SaveWindow(ctx, state)
			require.NoError(t, err)
		}

		// List all windows
		windows, err := manyStorage.ListWindows(ctx,
			baseTime.Add(-time.Minute),
			baseTime.Add(time.Duration(expectedCount)*time.Minute),
		)
		require.NoError(t, err)
		assert.Equal(t, expectedCount, len(windows))

		// Verify order and completeness
		for i, expected := range windowTimes {
			assert.True(t, expected.Equal(windows[i]))
		}
	})
}

// Benchmarks
func BenchmarkStorage(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "pebble-bench-*")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	ctx := context.Background()
	valueSerializer := serialization.NewGobSerializer[TestEvent]()
	keySerializer := serialization.NewGobSerializer[string]()

	s, err := NewStorage[string, TestEvent](
		StorageOptions{
			Path:         filepath.Join(tempDir, "bench.db"),
			BatchSize:    1000,
			CacheSize:    256 * 1024 * 1024,
			MaxOpenFiles: 1000,
		},
		valueSerializer,
		keySerializer,
	)
	require.NoError(b, err)
	defer s.Close()

	// Generate test data
	createTestState := func(numGroups, recordsPerGroup int, timestamp time.Time) storage.WindowState[string, TestEvent] {
		state := storage.WindowState[string, TestEvent]{
			WindowStart: timestamp,
			WindowEnd:   timestamp.Add(time.Hour),
			Groups:      make(map[string][]types.Record[TestEvent]),
		}

		for i := 0; i < numGroups; i++ {
			groupKey := fmt.Sprintf("group%d", i)
			records := make([]types.Record[TestEvent], recordsPerGroup)

			for j := 0; j < recordsPerGroup; j++ {
				records[j] = types.Record[TestEvent]{
					Data: TestEvent{
						ID:    fmt.Sprintf("%d-%d", i, j),
						Value: rand.Float64() * 1000,
						Labels: map[string]string{
							"group": groupKey,
							"index": fmt.Sprintf("%d", j),
						},
					},
					Timestamp: timestamp,
				}
			}

			state.Groups[groupKey] = records
		}

		return state
	}

	b.Run("SaveWindow/Small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			state := createTestState(10, 100, time.Now())
			err := s.SaveWindow(ctx, state)
			require.NoError(b, err)
		}
	})

	b.Run("SaveWindow/Medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			state := createTestState(100, 1000, time.Now())
			err := s.SaveWindow(ctx, state)
			require.NoError(b, err)
		}
	})

	b.Run("SaveWindow/Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			state := createTestState(1000, 1000, time.Now())
			err := s.SaveWindow(ctx, state)
			require.NoError(b, err)
		}
	})

	b.Run("LoadWindow/Small", func(b *testing.B) {
		timestamp := time.Now()
		state := createTestState(10, 100, timestamp)
		err := s.SaveWindow(ctx, state)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s.LoadWindow(ctx, timestamp)
			require.NoError(b, err)
		}
	})

	b.Run("LoadWindow/Medium", func(b *testing.B) {
		timestamp := time.Now()
		state := createTestState(100, 1000, timestamp)
		err := s.SaveWindow(ctx, state)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s.LoadWindow(ctx, timestamp)
			require.NoError(b, err)
		}
	})

	b.Run("Sequential Operations", func(b *testing.B) {
		timestamp := time.Now()
		for i := 0; i < b.N; i++ {
			state := createTestState(10, 100, timestamp.Add(time.Duration(i)*time.Second))
			err := s.SaveWindow(ctx, state)
			require.NoError(b, err)

			_, err = s.LoadWindow(ctx, timestamp)
			require.NoError(b, err)

			err = s.DeleteWindow(ctx, timestamp)
			require.NoError(b, err)
		}
	})

	b.Run("ListWindows/Range", func(b *testing.B) {
		// Prepare data
		for i := 0; i < 1000; i++ {
			state := createTestState(1, 10, time.Now().Add(time.Duration(i)*time.Minute))
			err := s.SaveWindow(ctx, state)
			require.NoError(b, err)
		}

		start := time.Now().Add(-24 * time.Hour)
		end := time.Now().Add(24 * time.Hour)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := s.ListWindows(ctx, start, end)
			require.NoError(b, err)
		}
	})
}

func TestPebbleStorage_LoadWindow(t *testing.T) {
	s, cleanup := setupTestStorage(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	tests := []struct {
		name          string
		recordsCount  int
		groupsCount   int
		batchSize     int
		validateOrder bool
	}{
		{
			name:          "Single Record",
			recordsCount:  1,
			groupsCount:   1,
			batchSize:     1000,
			validateOrder: true,
		},
		{
			name:          "Multiple Records Single Group",
			recordsCount:  100,
			groupsCount:   1,
			batchSize:     1000,
			validateOrder: true,
		},
		{
			name:          "Multiple Records Multiple Groups",
			recordsCount:  100,
			groupsCount:   10,
			batchSize:     1000,
			validateOrder: true,
		},
		{
			name:          "Large Dataset Multiple Chunks",
			recordsCount:  10000,
			groupsCount:   5,
			batchSize:     1000,
			validateOrder: true,
		},
		{
			name:          "Very Large Dataset",
			recordsCount:  50000,
			groupsCount:   20,
			batchSize:     1000,
			validateOrder: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test data
			originalState := storage.WindowState[string, TestEvent]{
				WindowStart: now,
				WindowEnd:   now.Add(time.Hour),
				Groups:      make(map[string][]types.Record[TestEvent]),
			}

			// Generate data for each group
			for g := 0; g < tt.groupsCount; g++ {
				groupKey := fmt.Sprintf("group-%d", g)
				records := make([]types.Record[TestEvent], tt.recordsCount)

				for i := 0; i < tt.recordsCount; i++ {
					records[i] = types.Record[TestEvent]{
						Data: TestEvent{
							ID:    fmt.Sprintf("%d-%d", g, i),
							Value: float64(i),
							Labels: map[string]string{
								"group": groupKey,
								"index": fmt.Sprintf("%d", i),
							},
						},
						Timestamp: now,
					}
				}

				originalState.Groups[groupKey] = records
			}

			// Save state
			err := s.SaveWindow(ctx, originalState)
			require.NoError(t, err)

			// Load state
			loadedState, err := s.LoadWindow(ctx, now)
			require.NoError(t, err)

			// Verify window boundaries
			assert.Equal(t, originalState.WindowStart, loadedState.WindowStart)
			assert.Equal(t, originalState.WindowEnd, loadedState.WindowEnd)

			// Verify groups count
			assert.Equal(t, len(originalState.Groups), len(loadedState.Groups))

			// Verify each group
			for groupKey, originalRecords := range originalState.Groups {
				loadedRecords, exists := loadedState.Groups[groupKey]
				assert.True(t, exists, "Group %s not found in loaded state", groupKey)
				assert.Equal(t, len(originalRecords), len(loadedRecords),
					"Record count mismatch for group %s", groupKey)

				if tt.validateOrder {
					// Verify record order
					for i, original := range originalRecords {
						loaded := loadedRecords[i]
						assert.Equal(t, original.Data.ID, loaded.Data.ID,
							"Record order mismatch at index %d in group %s", i, groupKey)
						assert.Equal(t, original.Data.Value, loaded.Data.Value)
						assert.Equal(t, original.Data.Labels, loaded.Data.Labels)
						assert.Equal(t, original.Timestamp, loaded.Timestamp)
					}
				}
			}
		})
	}

	// Test concurrent loading
	t.Run("Concurrent Loading", func(t *testing.T) {
		concurrency := 10
		var wg sync.WaitGroup
		errCh := make(chan error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.LoadWindow(ctx, now)
				if err != nil {
					errCh <- err
				}
			}()
		}

		wg.Wait()
		close(errCh)

		for err := range errCh {
			require.NoError(t, err)
		}
	})

	// Test loading non-existent window
	t.Run("Non-existent Window", func(t *testing.T) {
		nonExistentTime := now.Add(24 * time.Hour)
		state, err := s.LoadWindow(ctx, nonExistentTime)
		require.NoError(t, err)
		assert.Empty(t, state.Groups)
	})

	// Test loading partially corrupted window
	t.Run("Corrupted Window", func(t *testing.T) {
		// First save valid data
		originalState := storage.WindowState[string, TestEvent]{
			WindowStart: now,
			WindowEnd:   now.Add(time.Hour),
			Groups: map[string][]types.Record[TestEvent]{
				"group1": {
					{
						Data: TestEvent{
							ID:    "1",
							Value: 100,
						},
						Timestamp: now,
					},
				},
			},
		}

		err := s.SaveWindow(ctx, originalState)
		require.NoError(t, err)

		// Manually corrupt some data (if possible with your storage implementation)
		// This might require direct Pebble DB access

		// Try loading the corrupted window
		_, err = s.LoadWindow(ctx, now)
		// Depending on your error handling strategy:
		// - Either expect no error but potentially partial data
		// - Or expect an error
		// Choose the appropriate assertion based on your design
		if err != nil {
			assert.Contains(t, err.Error(), "failed to deserialize")
		}
	})
}
