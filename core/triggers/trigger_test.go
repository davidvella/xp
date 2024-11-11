package triggers

import (
	"testing"
	"time"

	"github.com/davidvella/xp/core/types"
	"github.com/stretchr/testify/assert"
)

type TestEvent struct {
	ID    string
	Value float64
}

func TestCountTrigger(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		setup     func(*testing.T) TriggerContext[TestEvent]
		expected  bool
	}{
		{
			name:      "Should not fire below threshold",
			threshold: 3,
			setup: func(t *testing.T) TriggerContext[TestEvent] {
				return TriggerContext[TestEvent]{
					WindowStart: time.Now(),
					CurrentData: []types.Record[TestEvent]{
						{Data: TestEvent{ID: "1"}},
						{Data: TestEvent{ID: "2"}},
					},
				}
			},
			expected: false,
		},
		{
			name:      "Should fire at threshold",
			threshold: 3,
			setup: func(t *testing.T) TriggerContext[TestEvent] {
				return TriggerContext[TestEvent]{
					WindowStart: time.Now(),
					CurrentData: []types.Record[TestEvent]{
						{Data: TestEvent{ID: "1"}},
						{Data: TestEvent{ID: "2"}},
						{Data: TestEvent{ID: "3"}},
					},
				}
			},
			expected: true,
		},
		{
			name:      "Should handle multiple windows independently",
			threshold: 2,
			setup: func(t *testing.T) TriggerContext[TestEvent] {
				return TriggerContext[TestEvent]{
					WindowStart: time.Now().Add(time.Hour),
					CurrentData: []types.Record[TestEvent]{
						{Data: TestEvent{ID: "1"}},
					},
				}
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewCountTrigger[TestEvent](tt.threshold)
			ctx := tt.setup(t)

			// Test OnElement
			result := trigger.OnElement(ctx)
			assert.Equal(t, tt.expected, result)

			// Test Clear
			trigger.Clear(ctx.WindowStart)
			result = trigger.OnElement(ctx)
			assert.False(t, result, "Should reset count after Clear")

			// Verify other methods always return false
			assert.False(t, trigger.OnProcessingTime(ctx))
			assert.False(t, trigger.OnEventTime(ctx))
		})
	}
}

func TestProcessingTimeTrigger(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		setup    func() (TriggerContext[TestEvent], TriggerContext[TestEvent])
		expected bool
	}{
		{
			name:     "Should not fire before interval",
			interval: time.Second,
			setup: func() (TriggerContext[TestEvent], TriggerContext[TestEvent]) {
				now := time.Now()
				ctx1 := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now,
				}
				ctx2 := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now.Add(500 * time.Millisecond),
				}
				return ctx1, ctx2
			},
			expected: false,
		},
		{
			name:     "Should fire after interval",
			interval: time.Second,
			setup: func() (TriggerContext[TestEvent], TriggerContext[TestEvent]) {
				now := time.Now()
				ctx1 := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now,
				}
				ctx2 := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now.Add(1100 * time.Millisecond),
				}
				return ctx1, ctx2
			},
			expected: true,
		},
		{
			name:     "Should handle multiple windows independently",
			interval: time.Second,
			setup: func() (TriggerContext[TestEvent], TriggerContext[TestEvent]) {
				now := time.Now()
				ctx1 := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now,
				}
				ctx2 := TriggerContext[TestEvent]{
					WindowStart:    now.Add(time.Hour),
					ProcessingTime: now,
				}
				return ctx1, ctx2
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger := NewProcessingTimeTrigger[TestEvent](tt.interval)
			ctx1, ctx2 := tt.setup()

			// First call should initialize
			assert.False(t, trigger.OnProcessingTime(ctx1))

			// Second call should match expected behavior
			result := trigger.OnProcessingTime(ctx2)
			assert.Equal(t, tt.expected, result)

			// Test Clear
			trigger.Clear(ctx1.WindowStart)
			result = trigger.OnProcessingTime(ctx1)
			assert.False(t, result, "Should reset after Clear")

			// Verify other methods always return false
			assert.False(t, trigger.OnElement(ctx1))
			assert.False(t, trigger.OnEventTime(ctx1))
		})
	}
}

func TestCompositeTrigger(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() (Trigger[TestEvent], TriggerContext[TestEvent])
		expected bool
	}{
		{
			name: "Should fire when any trigger fires (count)",
			setup: func() (Trigger[TestEvent], TriggerContext[TestEvent]) {
				countTrigger := NewCountTrigger[TestEvent](2)
				timeTrigger := NewProcessingTimeTrigger[TestEvent](time.Hour)
				composite := NewCompositeTrigger(countTrigger, timeTrigger)

				ctx := TriggerContext[TestEvent]{
					WindowStart: time.Now(),
					CurrentData: []types.Record[TestEvent]{
						{Data: TestEvent{ID: "1"}},
						{Data: TestEvent{ID: "2"}},
					},
				}
				return composite, ctx
			},
			expected: true,
		},
		{
			name: "Should fire when any trigger fires (time)",
			setup: func() (Trigger[TestEvent], TriggerContext[TestEvent]) {
				now := time.Now()
				countTrigger := NewCountTrigger[TestEvent](10)
				timeTrigger := NewProcessingTimeTrigger[TestEvent](time.Second)
				composite := NewCompositeTrigger(countTrigger, timeTrigger)

				// Initialize time trigger
				_ = timeTrigger.OnProcessingTime(TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now,
				})

				ctx := TriggerContext[TestEvent]{
					WindowStart:    now,
					ProcessingTime: now.Add(2 * time.Second),
				}
				return composite, ctx
			},
			expected: true,
		},
		{
			name: "Should not fire when no triggers fire",
			setup: func() (Trigger[TestEvent], TriggerContext[TestEvent]) {
				countTrigger := NewCountTrigger[TestEvent](10)
				timeTrigger := NewProcessingTimeTrigger[TestEvent](time.Hour)
				composite := NewCompositeTrigger(countTrigger, timeTrigger)

				ctx := TriggerContext[TestEvent]{
					WindowStart:    time.Now(),
					ProcessingTime: time.Now(),
					CurrentData: []types.Record[TestEvent]{
						{Data: TestEvent{ID: "1"}},
					},
				}
				return composite, ctx
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trigger, ctx := tt.setup()

			// Test OnElement
			elementResult := trigger.OnElement(ctx)
			// Test OnProcessingTime
			processingResult := trigger.OnProcessingTime(ctx)
			// Test OnEventTime
			eventResult := trigger.OnEventTime(ctx)

			// Any trigger should fire if expected is true
			assert.Equal(t, tt.expected, elementResult || processingResult || eventResult)

			// Test Clear
			trigger.Clear(ctx.WindowStart)
			elementResult = trigger.OnElement(ctx)
			processingResult = trigger.OnProcessingTime(ctx)
			eventResult = trigger.OnEventTime(ctx)
			assert.False(t, elementResult || processingResult || eventResult,
				"Should reset all triggers after Clear")
		})
	}
}

func BenchmarkTriggers(b *testing.B) {
	b.Run("CountTrigger", func(b *testing.B) {
		trigger := NewCountTrigger[TestEvent](1000)
		ctx := TriggerContext[TestEvent]{
			WindowStart: time.Now(),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			trigger.OnElement(ctx)
		}
	})

	b.Run("ProcessingTimeTrigger", func(b *testing.B) {
		trigger := NewProcessingTimeTrigger[TestEvent](time.Second)
		baseTime := time.Now()
		ctx := TriggerContext[TestEvent]{
			WindowStart: baseTime,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx.ProcessingTime = baseTime.Add(time.Duration(i) * time.Millisecond)
			trigger.OnProcessingTime(ctx)
		}
	})

	b.Run("CompositeTrigger", func(b *testing.B) {
		countTrigger := NewCountTrigger[TestEvent](1000)
		timeTrigger := NewProcessingTimeTrigger[TestEvent](time.Second)
		trigger := NewCompositeTrigger(countTrigger, timeTrigger)
		baseTime := time.Now()
		ctx := TriggerContext[TestEvent]{
			WindowStart: baseTime,
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctx.ProcessingTime = baseTime.Add(time.Duration(i) * time.Millisecond)
			trigger.OnElement(ctx)
			trigger.OnProcessingTime(ctx)
			trigger.OnEventTime(ctx)
		}
	})
}
