package benchmarks

import (
	"fmt"
	"testing"

	"github.com/rivulet-io/tower/op"
)

// Benchmark basic list operations
func BenchmarkCreateList(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("list:create:%d", i)
		if err := twr.CreateList(key); err != nil {
			b.Fatalf("CreateList failed: %v", err)
		}
	}
}

func BenchmarkDeleteList(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup lists
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("list:delete:%d", i)
		if err := twr.CreateList(key); err != nil {
			b.Fatalf("Setup CreateList failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("list:delete:%d", i)
		if err := twr.DeleteList(key); err != nil {
			b.Fatalf("DeleteList failed: %v", err)
		}
	}
}

// Benchmark list deque operations
func BenchmarkPushLeft(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:pushleft:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushLeft(key, value); err != nil {
			b.Fatalf("PushLeft failed: %v", err)
		}
	}
}

func BenchmarkPushRight(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:pushright:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("PushRight failed: %v", err)
		}
	}
}

func BenchmarkPopLeft(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:popleft:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.PopLeft(key); err != nil {
			b.Fatalf("PopLeft failed: %v", err)
		}
	}
}

func BenchmarkPopRight(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:popright:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.PopRight(key); err != nil {
			b.Fatalf("PopRight failed: %v", err)
		}
	}
}

// Benchmark list query operations
func BenchmarkListLength(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:length:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < 100; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ListLength(key); err != nil {
			b.Fatalf("ListLength failed: %v", err)
		}
	}
}

func BenchmarkListIndex(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:index:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < 100; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := int64(i % 100)
		if _, err := twr.ListIndex(key, index); err != nil {
			b.Fatalf("ListIndex failed: %v", err)
		}
	}
}

func BenchmarkListSet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:set:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < 100; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := int64(i % 100)
		value := op.PrimitiveInt(int64(i * 10))
		if err := twr.ListSet(key, index, value); err != nil {
			b.Fatalf("ListSet failed: %v", err)
		}
	}
}

func BenchmarkListRange(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:range:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < 100; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := int64(i % 90)
		end := start + 10
		if _, err := twr.ListRange(key, start, end); err != nil {
			b.Fatalf("ListRange failed: %v", err)
		}
	}
}

func BenchmarkListTrim(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:trim:benchmark"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create and populate list for each iteration
		if err := twr.CreateList(key); err != nil {
			b.Fatalf("CreateList failed: %v", err)
		}

		for j := 0; j < 100; j++ {
			value := op.PrimitiveInt(int64(j))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("Setup PushRight failed: %v", err)
			}
		}

		// Trim the list
		if err := twr.ListTrim(key, 10, 89); err != nil {
			b.Fatalf("ListTrim failed: %v", err)
		}

		// Cleanup for next iteration
		if err := twr.DeleteList(key); err != nil {
			b.Fatalf("Cleanup failed: %v", err)
		}
	}
}

// Benchmark list operations with different data types
func BenchmarkListWithStrings(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:strings:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveString(fmt.Sprintf("string_%d", i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("PushRight with string failed: %v", err)
		}
	}
}

func BenchmarkListWithFloats(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:floats:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveFloat(float64(i) + 0.5)
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("PushRight with float failed: %v", err)
		}
	}
}

func BenchmarkListWithBools(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:bools:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := op.PrimitiveBool(i%2 == 0)
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("PushRight with bool failed: %v", err)
		}
	}
}

// Benchmark concurrent list operations
func BenchmarkConcurrentPushRight(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numLists := 100
	for i := 0; i < numLists; i++ {
		key := fmt.Sprintf("list:concurrent:push:%d", i)
		if err := twr.CreateList(key); err != nil {
			b.Fatalf("Setup CreateList failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("list:concurrent:push:%d", i%numLists)
			value := op.PrimitiveInt(int64(i))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("ConcurrentPushRight failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentPopLeft(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numLists := 10
	itemsPerList := b.N / numLists
	if itemsPerList < 1 {
		itemsPerList = 1
	}

	// Setup lists with items
	for i := 0; i < numLists; i++ {
		key := fmt.Sprintf("list:concurrent:pop:%d", i)
		if err := twr.CreateList(key); err != nil {
			b.Fatalf("Setup CreateList failed: %v", err)
		}

		for j := 0; j < itemsPerList*2; j++ { // Extra items to avoid empty list errors
			value := op.PrimitiveInt(int64(j))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("Setup PushRight failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("list:concurrent:pop:%d", i%numLists)
			if _, err := twr.PopLeft(key); err != nil {
				// Continue on empty list errors as this can happen in concurrent scenarios
				if err.Error() != "list is empty" {
					b.Fatalf("ConcurrentPopLeft failed: %v", err)
				}
			}
			i++
		}
	})
}

// Benchmark list as queue and stack
func BenchmarkListAsQueue(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:queue:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Queue operations: PushRight (enqueue), PopLeft (dequeue)
		if i%2 == 0 {
			value := op.PrimitiveInt(int64(i))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("Queue PushRight failed: %v", err)
			}
		} else {
			if _, err := twr.PopLeft(key); err != nil {
				// Handle empty queue gracefully
				if err.Error() != "list is empty" {
					b.Fatalf("Queue PopLeft failed: %v", err)
				}
			}
		}
	}
}

func BenchmarkListAsStack(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "list:stack:benchmark"
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stack operations: PushRight (push), PopRight (pop)
		if i%2 == 0 {
			value := op.PrimitiveInt(int64(i))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("Stack PushRight failed: %v", err)
			}
		} else {
			if _, err := twr.PopRight(key); err != nil {
				// Handle empty stack gracefully
				if err.Error() != "list is empty" {
					b.Fatalf("Stack PopRight failed: %v", err)
				}
			}
		}
	}
}

// Benchmark list operations by size
func BenchmarkSmallListOperations(b *testing.B) {
	benchmarkListOperationsBySize(b, "small", 10)
}

func BenchmarkMediumListOperations(b *testing.B) {
	benchmarkListOperationsBySize(b, "medium", 100)
}

func BenchmarkLargeListOperations(b *testing.B) {
	benchmarkListOperationsBySize(b, "large", 1000)
}

func benchmarkListOperationsBySize(b *testing.B, size string, listSize int) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := fmt.Sprintf("list:size:%s", size)
	if err := twr.CreateList(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate list
	for i := 0; i < listSize; i++ {
		value := op.PrimitiveInt(int64(i))
		if _, err := twr.PushRight(key, value); err != nil {
			b.Fatalf("Setup PushRight failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			if _, err := twr.ListLength(key); err != nil {
				b.Fatalf("ListLength failed: %v", err)
			}
		case 1:
			index := int64(i % listSize)
			if _, err := twr.ListIndex(key, index); err != nil {
				b.Fatalf("ListIndex failed: %v", err)
			}
		case 2:
			value := op.PrimitiveInt(int64(i))
			if _, err := twr.PushRight(key, value); err != nil {
				b.Fatalf("PushRight failed: %v", err)
			}
		case 3:
			if _, err := twr.PopLeft(key); err != nil {
				// Handle empty list gracefully
				if err.Error() != "list is empty" {
					b.Fatalf("PopLeft failed: %v", err)
				}
			}
		}
	}
}
