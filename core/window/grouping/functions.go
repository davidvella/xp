package grouping

import "github.com/davidvella/xp/core/types"

// GroupFunction defines how records should be grouped
type GroupFunction[K types.GroupKey, T any] interface {
	// GetKey extracts the group key from a record
	GetKey(record types.Record[T]) K
}

// SimpleGroupFunction groups by a string key
type SimpleGroupFunction[T any] struct {
	keyFn func(T) string
}

func NewSimpleGroupFunction[T any](keyFn func(T) string) *SimpleGroupFunction[T] {
	return &SimpleGroupFunction[T]{keyFn: keyFn}
}

func (g *SimpleGroupFunction[T]) GetKey(record types.Record[T]) string {
	return g.keyFn(record.Data)
}

// CompositeGroupFunction allows grouping by multiple fields
type CompositeKey struct {
	Fields map[string]interface{}
}

type CompositeGroupFunction[T any] struct {
	fieldExtractors map[string]func(T) interface{}
}

func NewCompositeGroupFunction[T any]() *CompositeGroupFunction[T] {
	return &CompositeGroupFunction[T]{
		fieldExtractors: make(map[string]func(T) interface{}),
	}
}

func (g *CompositeGroupFunction[T]) AddField(name string, extractor func(T) interface{}) {
	g.fieldExtractors[name] = extractor
}

func (g *CompositeGroupFunction[T]) GetKey(record types.Record[T]) CompositeKey {
	fields := make(map[string]interface{})
	for name, extractor := range g.fieldExtractors {
		fields[name] = extractor(record.Data)
	}
	return CompositeKey{Fields: fields}
}
