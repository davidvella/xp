package triggers

import (
	"time"

	"github.com/davidvella/xp/core/types"
)

// TriggerContext provides information for trigger evaluation
type TriggerContext[T any] struct {
	WindowStart    time.Time
	WindowEnd      time.Time
	CurrentData    []types.Record[T]
	EventTime      time.Time
	ProcessingTime time.Time
	Watermark      time.Time
}

// Trigger defines when windows should be evaluated/emitted
type Trigger[T any] interface {
	// OnElement is called for each new element
	OnElement(ctx TriggerContext[T]) bool
	// OnProcessingTime is called on processing time timer
	OnProcessingTime(ctx TriggerContext[T]) bool
	// OnEventTime is called on event time timer
	OnEventTime(ctx TriggerContext[T]) bool
	// Clear any state after firing
	Clear(time.Time)
}

// CompositeTrigger combines multiple triggers
type CompositeTrigger[T any] struct {
	triggers []Trigger[T]
}

func NewCompositeTrigger[T any](triggers ...Trigger[T]) *CompositeTrigger[T] {
	return &CompositeTrigger[T]{triggers: triggers}
}

func (ct *CompositeTrigger[T]) OnElement(ctx TriggerContext[T]) bool {
	shouldFire := false
	for _, t := range ct.triggers {
		if t.OnElement(ctx) {
			shouldFire = true
		}
	}
	return shouldFire
}

func (ct *CompositeTrigger[T]) OnProcessingTime(ctx TriggerContext[T]) bool {
	shouldFire := false
	for _, t := range ct.triggers {
		if t.OnProcessingTime(ctx) {
			shouldFire = true
		}
	}
	return shouldFire
}

func (ct *CompositeTrigger[T]) OnEventTime(ctx TriggerContext[T]) bool {
	shouldFire := false
	for _, t := range ct.triggers {
		if t.OnEventTime(ctx) {
			shouldFire = true
		}
	}
	return shouldFire
}

func (ct *CompositeTrigger[T]) Clear(windowTime time.Time) {
	for _, t := range ct.triggers {
		t.Clear(windowTime)
	}
}

// CountTrigger fires after receiving N elements
type CountTrigger[T any] struct {
	threshold int
	counts    map[time.Time]int
}

func NewCountTrigger[T any](threshold int) *CountTrigger[T] {
	return &CountTrigger[T]{
		threshold: threshold,
		counts:    make(map[time.Time]int),
	}
}

func (ct *CountTrigger[T]) OnElement(ctx TriggerContext[T]) bool {
	ct.counts[ctx.WindowStart]++
	return ct.counts[ctx.WindowStart] >= ct.threshold
}

func (ct *CountTrigger[T]) OnProcessingTime(ctx TriggerContext[T]) bool {
	return false
}

func (ct *CountTrigger[T]) OnEventTime(ctx TriggerContext[T]) bool {
	return false
}

func (ct *CountTrigger[T]) Clear(windowTime time.Time) {
	delete(ct.counts, windowTime)
}

// ProcessingTimeTrigger fires based on processing time
type ProcessingTimeTrigger[T any] struct {
	interval time.Duration
	lastFire map[time.Time]time.Time
}

func NewProcessingTimeTrigger[T any](interval time.Duration) *ProcessingTimeTrigger[T] {
	return &ProcessingTimeTrigger[T]{
		interval: interval,
		lastFire: make(map[time.Time]time.Time),
	}
}

func (pt *ProcessingTimeTrigger[T]) OnElement(ctx TriggerContext[T]) bool {
	return false
}

func (pt *ProcessingTimeTrigger[T]) OnProcessingTime(ctx TriggerContext[T]) bool {
	last, ok := pt.lastFire[ctx.WindowStart]
	if !ok {
		pt.lastFire[ctx.WindowStart] = ctx.ProcessingTime
		return false
	}

	if ctx.ProcessingTime.Sub(last) >= pt.interval {
		pt.lastFire[ctx.WindowStart] = ctx.ProcessingTime
		return true
	}
	return false
}

func (pt *ProcessingTimeTrigger[T]) OnEventTime(ctx TriggerContext[T]) bool {
	return false
}

func (pt *ProcessingTimeTrigger[T]) Clear(windowTime time.Time) {
	delete(pt.lastFire, windowTime)
}
