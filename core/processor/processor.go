package processor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/davidvella/xp/core/aggregation"
	"github.com/davidvella/xp/core/monitoring"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/triggers"
	"github.com/davidvella/xp/core/types"
	"github.com/davidvella/xp/core/window"
	"github.com/davidvella/xp/core/window/grouping"
)

var (
	ErrProcessorClosed = errors.New("processor is closed")
	ErrInvalidRecord   = errors.New("invalid record")
)

// ProcessorConfig holds configuration for the processor
type ProcessorConfig struct {
	MaxBufferSize     int
	ProcessingTimeout time.Duration
	WatermarkInterval time.Duration
	CleanupInterval   time.Duration
}

// DefaultConfig returns default processor configuration
func DefaultConfig() ProcessorConfig {
	return ProcessorConfig{
		MaxBufferSize:     10000,
		ProcessingTimeout: 1 * time.Minute,
		WatermarkInterval: 1 * time.Second,
		CleanupInterval:   5 * time.Minute,
	}
}

// ProcessorResult represents the result of window processing
type ProcessorResult[K types.GroupKey, R any] struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Results     map[K]R
	Metadata    map[string]interface{}
}

// Processor handles the windowing, grouping, and aggregation of data
type Processor[K types.GroupKey, T any, R any] struct {
	// Core components
	strategy    window.WindowStrategy[T]
	groupFn     grouping.GroupFunction[K, T]
	aggregateFn aggregation.AggregateFunction[K, T, R]
	trigger     triggers.Trigger[T]
	storage     storage.Storage[K, T]

	// Monitoring
	stats  monitoring.Stats
	logger monitoring.Logger

	// Configuration
	config ProcessorConfig

	// Internal state
	watermark  time.Time
	inputChan  chan types.Record[T]
	outputChan chan ProcessorResult[K, R]
	errChan    chan error

	// Lifecycle management
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.RWMutex
	closed     bool
}

// NewProcessor creates a new processor instance
func NewProcessor[K types.GroupKey, T any, R any](
	strategy window.WindowStrategy[T],
	groupFn grouping.GroupFunction[K, T],
	aggregateFn aggregation.AggregateFunction[K, T, R],
	trigger triggers.Trigger[T],
	storage storage.Storage[K, T],
	stats monitoring.Stats,
	logger monitoring.Logger,
	config ProcessorConfig,
) *Processor[K, T, R] {
	ctx, cancel := context.WithCancel(context.Background())

	return &Processor[K, T, R]{
		strategy:    strategy,
		groupFn:     groupFn,
		aggregateFn: aggregateFn,
		trigger:     trigger,
		storage:     storage,
		stats:       stats,
		logger:      logger,
		config:      config,
		watermark:   time.Now(),
		inputChan:   make(chan types.Record[T], config.MaxBufferSize),
		outputChan:  make(chan ProcessorResult[K, R], config.MaxBufferSize),
		errChan:     make(chan error, config.MaxBufferSize),
		ctx:         ctx,
		cancelFunc:  cancel,
	}
}

// Start begins processing records
func (p *Processor[K, T, R]) Start() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrProcessorClosed
	}
	p.closed = false
	p.mu.Unlock()

	// Start processing goroutine
	p.wg.Add(1)
	go p.processLoop()

	// Start watermark update goroutine
	p.wg.Add(1)
	go p.watermarkLoop()

	// Start cleanup goroutine
	p.wg.Add(1)
	go p.cleanupLoop()

	p.logger.Log(p.ctx, monitoring.INFO, "processor_started", "Processor started", nil)
	return nil
}

// Stop gracefully shuts down the processor
func (p *Processor[K, T, R]) Stop() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrProcessorClosed
	}
	p.closed = true
	p.mu.Unlock()

	// Signal shutdown
	p.cancelFunc()

	// Wait for all goroutines to finish
	p.wg.Wait()

	// Close channels
	close(p.inputChan)
	close(p.outputChan)
	close(p.errChan)

	p.logger.Log(p.ctx, monitoring.INFO, "processor_stopped", "Processor stopped", nil)
	return nil
}

// Process submits a record for processing
func (p *Processor[K, T, R]) Process(ctx context.Context, record types.Record[T]) error {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return ErrProcessorClosed
	}
	p.mu.RUnlock()

	// Validate record
	if err := p.validateRecord(record); err != nil {
		p.stats.RecordProcessError(ctx, "validation_error")
		return err
	}

	// Submit record with timeout
	select {
	case p.inputChan <- record:
		p.stats.RecordProcessedRecord(ctx, map[string]string{
			"event_time": record.Timestamp.Format(time.RFC3339),
		})
		return nil
	case <-ctx.Done():
		p.stats.RecordProcessError(ctx, "context_cancelled")
		return ctx.Err()
	case <-p.ctx.Done():
		return ErrProcessorClosed
	}
}

// Output returns the channel for processed results
func (p *Processor[K, T, R]) Output() <-chan ProcessorResult[K, R] {
	return p.outputChan
}

// Errors returns the channel for processing errors
func (p *Processor[K, T, R]) Errors() <-chan error {
	return p.errChan
}

// internal processing loop
func (p *Processor[K, T, R]) processLoop() {
	defer p.wg.Done()

	for {
		select {
		case record := <-p.inputChan:
			if err := p.processRecord(p.ctx, record); err != nil {
				p.handleError(err)
			}
		case <-p.ctx.Done():
			return
		}
	}
}

// processRecord handles a single record
func (p *Processor[K, T, R]) processRecord(ctx context.Context, record types.Record[T]) error {
	start := time.Now()
	defer func() {
		p.stats.RecordProcessingLatency(ctx, time.Since(start), nil)
	}()

	// Assign windows
	windows := p.strategy.AssignWindows(record)
	key := p.groupFn.GetKey(record)

	for _, windowStart := range windows {
		if err := p.processWindow(ctx, windowStart, key, record); err != nil {
			return err
		}
	}

	return nil
}

// processWindow handles a single window
func (p *Processor[K, T, R]) processWindow(ctx context.Context, windowStart time.Time, key K, record types.Record[T]) error {
	// Load window state
	state, err := p.storage.LoadWindow(ctx, windowStart)
	if err != nil {
		return fmt.Errorf("failed to load window state: %w", err)
	}

	// Initialize window state if needed
	if state.Groups == nil {
		state.Groups = make(map[K][]types.Record[T])
	}

	// Add record to window
	state.Groups[key] = append(state.Groups[key], record)

	// Check trigger
	triggerCtx := triggers.TriggerContext[T]{
		WindowStart:    windowStart,
		WindowEnd:      windowStart.Add(time.Hour), // TODO: Get from strategy
		CurrentData:    state.Groups[key],
		EventTime:      record.Timestamp,
		ProcessingTime: time.Now(),
		Watermark:      p.watermark,
	}

	if p.trigger.OnElement(triggerCtx) {
		// Process and emit window
		if err := p.emitWindow(ctx, state); err != nil {
			return err
		}
		p.trigger.Clear(windowStart)
	} else {
		// Save updated state
		if err := p.storage.SaveWindow(ctx, state); err != nil {
			return fmt.Errorf("failed to save window state: %w", err)
		}
	}

	return nil
}

// emitWindow processes and emits a complete window
func (p *Processor[K, T, R]) emitWindow(ctx context.Context, state storage.WindowState[K, T]) error {
	results := make(map[K]R)

	// Process each group
	for key, records := range state.Groups {
		acc := p.aggregateFn.CreateAccumulator()
		for _, record := range records {
			acc = p.aggregateFn.AddInput(acc, record)
		}
		results[key] = p.aggregateFn.GetResult(acc)
	}

	// Create result
	result := ProcessorResult[K, R]{
		WindowStart: state.WindowStart,
		WindowEnd:   state.WindowEnd,
		Results:     results,
		Metadata: map[string]interface{}{
			"record_count": len(state.Groups),
			"group_count":  len(results),
		},
	}

	// Emit result
	select {
	case p.outputChan <- result:
		p.stats.RecordWindowEmitted(ctx, map[string]string{
			"window_start": state.WindowStart.Format(time.RFC3339),
		})
	case <-ctx.Done():
		return ctx.Err()
	}

	// Cleanup window state
	if err := p.storage.DeleteWindow(ctx, state.WindowStart); err != nil {
		return fmt.Errorf("failed to delete window state: %w", err)
	}

	return nil
}

// watermarkLoop updates the watermark periodically
func (p *Processor[K, T, R]) watermarkLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.WatermarkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.updateWatermark()
		case <-p.ctx.Done():
			return
		}
	}
}

// cleanupLoop performs periodic cleanup of old windows
func (p *Processor[K, T, R]) cleanupLoop() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.cleanup()
		case <-p.ctx.Done():
			return
		}
	}
}

// updateWatermark updates the current watermark
func (p *Processor[K, T, R]) updateWatermark() {
	p.mu.Lock()
	p.watermark = time.Now() // TODO: Implement more sophisticated watermark tracking
	p.mu.Unlock()

	// Check for windows that can be processed due to watermark advancement
	p.processWatermarkTriggers()
}

// cleanup removes old window states
func (p *Processor[K, T, R]) cleanup() {
	ctx := p.ctx
	cutoff := time.Now().Add(-24 * time.Hour) // TODO: Make configurable

	windows, err := p.storage.ListWindows(ctx, time.Time{}, cutoff)
	if err != nil {
		p.handleError(fmt.Errorf("failed to list windows for cleanup: %w", err))
		return
	}

	for _, windowStart := range windows {
		if err := p.storage.DeleteWindow(ctx, windowStart); err != nil {
			p.handleError(fmt.Errorf("failed to delete old window: %w", err))
		}
	}
}

// processWatermarkTriggers checks for windows that should be triggered due to watermark
func (p *Processor[K, T, R]) processWatermarkTriggers() {
	ctx := p.ctx
	windows, err := p.storage.ListWindows(ctx, time.Time{}, p.watermark)
	if err != nil {
		p.handleError(fmt.Errorf("failed to list windows for watermark processing: %w", err))
		return
	}

	for _, windowStart := range windows {
		state, err := p.storage.LoadWindow(ctx, windowStart)
		if err != nil {
			p.handleError(fmt.Errorf("failed to load window for watermark processing: %w", err))
			continue
		}

		triggerCtx := triggers.TriggerContext[T]{
			WindowStart:    windowStart,
			WindowEnd:      windowStart.Add(time.Hour), // TODO: Get from strategy
			ProcessingTime: time.Now(),
			Watermark:      p.watermark,
		}

		if p.trigger.OnEventTime(triggerCtx) {
			if err := p.emitWindow(ctx, state); err != nil {
				p.handleError(err)
			}
			p.trigger.Clear(windowStart)
		}
	}
}

// validateRecord performs basic validation of a record
func (p *Processor[K, T, R]) validateRecord(record types.Record[T]) error {
	if record.Timestamp.IsZero() {
		return ErrInvalidRecord
	}
	return nil
}

// handleError sends an error to the error channel
func (p *Processor[K, T, R]) handleError(err error) {
	select {
	case p.errChan <- err:
	default:
		// Error channel is full, log the error
		p.logger.Log(p.ctx, monitoring.ERROR, "error_channel_full", "Error channel is full", map[string]interface{}{
			"error": err.Error(),
		})
	}
}
