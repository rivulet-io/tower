package benchmarks

import (
	"fmt"
	"testing"

	"github.com/rivulet-io/tower"
)

// Benchmark basic set operations
func BenchmarkCreateSet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("set:create:%d", i)
		if err := twr.CreateSet(key); err != nil {
			b.Fatalf("CreateSet failed: %v", err)
		}
	}
}

func BenchmarkDeleteSet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup sets
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("set:delete:%d", i)
		if err := twr.CreateSet(key); err != nil {
			b.Fatalf("Setup CreateSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("set:delete:%d", i)
		if err := twr.DeleteSet(key); err != nil {
			b.Fatalf("DeleteSet failed: %v", err)
		}
	}
}

// Benchmark set membership operations
func BenchmarkSetAdd(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:add:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd failed: %v", err)
		}
	}
}

func BenchmarkSetRemove(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:remove:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetRemove(key, member); err != nil {
			b.Fatalf("SetRemove failed: %v", err)
		}
	}
}

func BenchmarkSetAddDuplicates(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:add:duplicates:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Add initial member
	member := tower.PrimitiveString("duplicate_member")
	if _, err := twr.SetAdd(key, member); err != nil {
		b.Fatalf("Setup SetAdd failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Try to add the same member repeatedly
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd duplicate failed: %v", err)
		}
	}
}

func BenchmarkSetRemoveNonExistent(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:remove:nonexistent:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Add some members but try to remove different ones
	for i := 0; i < 10; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("existing_member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		removeMember := tower.PrimitiveString(fmt.Sprintf("nonexistent_member_%d", i))
		if _, err := twr.SetRemove(key, removeMember); err != nil {
			b.Fatalf("SetRemove nonexistent failed: %v", err)
		}
	}
}

// Benchmark set query operations
func BenchmarkSetIsMember(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:ismember:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set
	for i := 0; i < 100; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i%100))
		if _, err := twr.SetIsMember(key, member); err != nil {
			b.Fatalf("SetIsMember failed: %v", err)
		}
	}
}

func BenchmarkSetIsMemberNonExistent(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:ismember:nonexistent:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set with some members
	for i := 0; i < 100; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("existing_member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("nonexistent_member_%d", i))
		if _, err := twr.SetIsMember(key, member); err != nil {
			b.Fatalf("SetIsMember nonexistent failed: %v", err)
		}
	}
}

func BenchmarkSetCardinality(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:cardinality:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set
	for i := 0; i < 100; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetCardinality(key); err != nil {
			b.Fatalf("SetCardinality failed: %v", err)
		}
	}
}

func BenchmarkSetMembers(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:members:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set
	for i := 0; i < 100; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetMembers(key); err != nil {
			b.Fatalf("SetMembers failed: %v", err)
		}
	}
}

// Benchmark set operations with different data types
func BenchmarkSetWithStrings(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:strings:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("string_member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd with string failed: %v", err)
		}
	}
}

func BenchmarkSetWithInts(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:ints:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("int_member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd with int failed: %v", err)
		}
	}
}

func BenchmarkSetWithFloats(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:floats:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("float_member_%.1f", float64(i)+0.5))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd with float failed: %v", err)
		}
	}
}

func BenchmarkSetWithBools(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:bools:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("bool_member_%t", i%2 == 0))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd with bool failed: %v", err)
		}
	}
}

// Benchmark set operations with mixed data types
func BenchmarkSetMixedTypes(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:mixed:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var member tower.PrimitiveData
		switch i % 4 {
		case 0:
			member = tower.PrimitiveString(fmt.Sprintf("string_%d", i))
		case 1:
			member = tower.PrimitiveString(fmt.Sprintf("int_%d", i))
		case 2:
			member = tower.PrimitiveString(fmt.Sprintf("float_%.1f", float64(i)+0.5))
		case 3:
			member = tower.PrimitiveString(fmt.Sprintf("bool_%t", i%2 == 0))
		}

		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("SetAdd with mixed types failed: %v", err)
		}
	}
}

// Benchmark concurrent set operations
func BenchmarkConcurrentSetAdd(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numSets := 100
	for i := 0; i < numSets; i++ {
		key := fmt.Sprintf("set:concurrent:add:%d", i)
		if err := twr.CreateSet(key); err != nil {
			b.Fatalf("Setup CreateSet failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("set:concurrent:add:%d", i%numSets)
			member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
			if _, err := twr.SetAdd(key, member); err != nil {
				b.Fatalf("ConcurrentSetAdd failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentSetIsMember(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numSets := 10
	membersPerSet := 100

	// Setup sets with data
	for i := 0; i < numSets; i++ {
		key := fmt.Sprintf("set:concurrent:ismember:%d", i)
		if err := twr.CreateSet(key); err != nil {
			b.Fatalf("Setup CreateSet failed: %v", err)
		}

		for j := 0; j < membersPerSet; j++ {
			member := tower.PrimitiveString(fmt.Sprintf("member_%d", j))
			if _, err := twr.SetAdd(key, member); err != nil {
				b.Fatalf("Setup SetAdd failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("set:concurrent:ismember:%d", i%numSets)
			member := tower.PrimitiveString(fmt.Sprintf("member_%d", i%membersPerSet))
			if _, err := twr.SetIsMember(key, member); err != nil {
				b.Fatalf("ConcurrentSetIsMember failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark set operations by size
func BenchmarkSmallSetOperations(b *testing.B) {
	benchmarkSetOperationsBySize(b, "small", 10)
}

func BenchmarkMediumSetOperations(b *testing.B) {
	benchmarkSetOperationsBySize(b, "medium", 100)
}

func BenchmarkLargeSetOperations(b *testing.B) {
	benchmarkSetOperationsBySize(b, "large", 1000)
}

func benchmarkSetOperationsBySize(b *testing.B, size string, setSize int) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := fmt.Sprintf("set:size:%s", size)
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate set
	for i := 0; i < setSize; i++ {
		member := tower.PrimitiveString(fmt.Sprintf("member_%d", i))
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup SetAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			if _, err := twr.SetCardinality(key); err != nil {
				b.Fatalf("SetCardinality failed: %v", err)
			}
		case 1:
			member := tower.PrimitiveString(fmt.Sprintf("member_%d", i%setSize))
			if _, err := twr.SetIsMember(key, member); err != nil {
				b.Fatalf("SetIsMember failed: %v", err)
			}
		case 2:
			member := tower.PrimitiveString(fmt.Sprintf("new_member_%d", i))
			if _, err := twr.SetAdd(key, member); err != nil {
				b.Fatalf("SetAdd failed: %v", err)
			}
		case 3:
			if _, err := twr.SetMembers(key); err != nil {
				b.Fatalf("SetMembers failed: %v", err)
			}
		}
	}
}

// Benchmark set as tag system
func BenchmarkSetAsTagSystem(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:tags:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate with some common tags
	commonTags := []string{"important", "urgent", "work", "personal", "draft", "reviewed"}
	for _, tag := range commonTags {
		member := tower.PrimitiveString(tag)
		if _, err := twr.SetAdd(key, member); err != nil {
			b.Fatalf("Setup common tags failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate tag operations: 60% check tag, 30% add tag, 10% remove tag
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5: // Check tag (60%)
			tag := commonTags[i%len(commonTags)]
			member := tower.PrimitiveString(tag)
			if _, err := twr.SetIsMember(key, member); err != nil {
				b.Fatalf("Tag check failed: %v", err)
			}
		case 6, 7, 8: // Add tag (30%)
			tag := fmt.Sprintf("dynamic_tag_%d", i)
			member := tower.PrimitiveString(tag)
			if _, err := twr.SetAdd(key, member); err != nil {
				b.Fatalf("Tag add failed: %v", err)
			}
		case 9: // Remove tag (10%)
			tag := fmt.Sprintf("dynamic_tag_%d", i-10) // Remove older tags
			member := tower.PrimitiveString(tag)
			if _, err := twr.SetRemove(key, member); err != nil {
				b.Fatalf("Tag remove failed: %v", err)
			}
		}
	}
}

// Benchmark set as unique constraint
func BenchmarkSetAsUniqueConstraint(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "set:unique:benchmark"
	if err := twr.CreateSet(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate unique constraint checking
		id := tower.PrimitiveString(fmt.Sprintf("id_%d", i%1000)) // Limit to 1000 unique IDs
		
		// First check if ID already exists
		exists, err := twr.SetIsMember(key, id)
		if err != nil {
			b.Fatalf("Unique constraint check failed: %v", err)
		}
		
		// If not exists, add it
		if !exists {
			if _, err := twr.SetAdd(key, id); err != nil {
				b.Fatalf("Unique constraint add failed: %v", err)
			}
		}
	}
}