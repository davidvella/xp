package priority_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/davidvella/xp/core/priority"
)

func TestPriorityQueue(t *testing.T) {
	tests := []struct {
		name     string
		ops      []operation
		wantLen  int
		wantPeek interface{}
	}{
		{
			name: "basic min heap operations",
			ops: []operation{
				{opType: opSet, key: "a", value: 5},
				{opType: opSet, key: "b", value: 3},
				{opType: opSet, key: "c", value: 7},
			},
			wantLen:  3,
			wantPeek: 3,
		},
		{
			name: "update existing key",
			ops: []operation{
				{opType: opSet, key: "a", value: 5},
				{opType: opSet, key: "a", value: 2},
			},
			wantLen:  1,
			wantPeek: 2,
		},
		{
			name: "remove operations",
			ops: []operation{
				{opType: opSet, key: "a", value: 5},
				{opType: opSet, key: "b", value: 3},
				{opType: opSet, key: "c", value: 7},
				{opType: opRemove, key: "b"},
			},
			wantLen:  2,
			wantPeek: 5,
		},
		{
			name: "pop operations",
			ops: []operation{
				{opType: opSet, key: "a", value: 5},
				{opType: opSet, key: "b", value: 3},
				{opType: opSet, key: "c", value: 7},
				{opType: opPop},
				{opType: opPop},
			},
			wantLen:  1,
			wantPeek: 7,
		},
		{
			name: "empty queue operations",
			ops: []operation{
				{opType: opPop},
				{opType: opPeek},
			},
			wantLen:  0,
			wantPeek: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pq := priority.NewQueue[string, int](func(a, b int) bool {
				return a < b
			})

			for _, op := range tt.ops {
				switch op.opType {
				case opSet:
					pq.Set(op.key, op.value)
				case opRemove:
					pq.Remove(op.key)
				case opPop:
					_, _, _ = pq.Pop()
				}
			}

			if got := pq.Len(); got != tt.wantLen {
				t.Errorf("Len() = %v, want %v", got, tt.wantLen)
			}

			if tt.wantPeek != nil {
				_, val, ok := pq.Peek()
				if !ok {
					t.Error("Peek() returned not ok, want ok")
				}
				if val != tt.wantPeek {
					t.Errorf("Peek() value = %v, want %v", val, tt.wantPeek)
				}
			}
		})
	}
}

func TestPriorityQueueOrder(t *testing.T) {
	pq := priority.NewQueue[string, int](func(a, b int) bool {
		return a < b
	})

	input := []struct {
		key   string
		value int
	}{
		{"a", 5},
		{"b", 3},
		{"c", 7},
		{"d", 1},
		{"e", 4},
	}

	for _, in := range input {
		pq.Set(in.key, in.value)
	}

	want := []int{1, 3, 4, 5, 7}
	got := make([]int, 0, len(want))

	for pq.Len() > 0 {
		_, val, ok := pq.Pop()
		if !ok {
			t.Fatal("Pop() returned not ok")
		}
		got = append(got, val)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Pop() order = %v, want %v", got, want)
			break
		}
	}
}

type opType int

const (
	opSet opType = iota
	opRemove
	opPop
	opPeek
)

type operation struct {
	opType opType
	key    string
	value  int
}

func BenchmarkPriorityQueue(b *testing.B) {
	b.ReportAllocs()
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Set_%d", size), func(b *testing.B) {
			pq := priority.NewQueue[string, int](func(a, b int) bool {
				return a < b
			})

			// Pre-populate half of the items
			for i := 0; i < size/2; i++ {
				key := fmt.Sprintf("key-%d", i)
				pq.Set(key, rand.Intn(10000))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key-%d", i)
				pq.Set(key, rand.Intn(10000))
			}
		})

		b.Run(fmt.Sprintf("Pop_%d", size), func(b *testing.B) {
			pq := priority.NewQueue[string, int](func(a, b int) bool {
				return a < b
			})

			// Pre-populate items
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key-%d", i)
				pq.Set(key, rand.Intn(10000))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if pq.Len() == 0 {
					b.StopTimer()
					// Repopulate when empty
					for j := 0; j < size; j++ {
						key := fmt.Sprintf("key-%d", j)
						pq.Set(key, rand.Intn(10000))
					}
					b.StartTimer()
				}
				_, _, _ = pq.Pop()
			}
		})

		b.Run(fmt.Sprintf("Mixed_%d", size), func(b *testing.B) {
			pq := priority.NewQueue[string, int](func(a, b int) bool {
				return a < b
			})

			// Pre-populate items
			for i := 0; i < size; i++ {
				key := fmt.Sprintf("key-%d", i)
				pq.Set(key, rand.Intn(10000))
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				switch rand.Intn(3) {
				case 0:
					key := fmt.Sprintf("key-%d", rand.Intn(size))
					pq.Set(key, rand.Intn(10000))
				case 1:
					if pq.Len() > 0 {
						_, _, _ = pq.Pop()
					}
				case 2:
					if pq.Len() > 0 {
						key := fmt.Sprintf("key-%d", rand.Intn(size))
						pq.Remove(key)
					}
				}
			}
		})
	}
}
