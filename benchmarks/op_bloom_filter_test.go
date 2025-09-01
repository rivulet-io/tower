package benchmarks

import (
	"fmt"
	"testing"
)

// Benchmark Bloom filter creation
func BenchmarkCreateBloomFilter(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bloom:create:%d", i)
		if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
			b.Fatalf("CreateBloomFilter failed: %v", err)
		}
	}
}

// Benchmark Bloom filter add operations
func BenchmarkBloomFilterAdd(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:add:benchmark"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := fmt.Sprintf("item_%d", i)
		if err := twr.BloomFilterAdd(key, item); err != nil {
			b.Fatalf("BloomFilterAdd failed: %v", err)
		}
	}
}

// Benchmark Bloom filter contains operations (existing items)
func BenchmarkBloomFilterContainsExisting(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:contains:existing"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	// Setup data
	numItems := 1000
	items := make([]string, numItems)
	for i := 0; i < numItems; i++ {
		items[i] = fmt.Sprintf("item_%d", i)
		if err := twr.BloomFilterAdd(key, items[i]); err != nil {
			b.Fatalf("Setup BloomFilterAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := items[i%numItems]
		if _, err := twr.BloomFilterContains(key, item); err != nil {
			b.Fatalf("BloomFilterContains failed: %v", err)
		}
	}
}

// Benchmark Bloom filter contains operations (non-existing items)
func BenchmarkBloomFilterContainsNonExisting(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:contains:nonexisting"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	// Setup data
	numItems := 1000
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("existing_%d", i)
		if err := twr.BloomFilterAdd(key, item); err != nil {
			b.Fatalf("Setup BloomFilterAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := fmt.Sprintf("nonexisting_%d", i)
		if _, err := twr.BloomFilterContains(key, item); err != nil {
			b.Fatalf("BloomFilterContains failed: %v", err)
		}
	}
}

// Benchmark Bloom filter clear
func BenchmarkBloomFilterClear(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:clear:benchmark"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	// Setup data
	numItems := 100
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("item_%d", i)
		if err := twr.BloomFilterAdd(key, item); err != nil {
			b.Fatalf("Setup BloomFilterAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := twr.BloomFilterClear(key); err != nil {
			b.Fatalf("BloomFilterClear failed: %v", err)
		}
		// Re-add items for next iteration
		for j := 0; j < numItems; j++ {
			item := fmt.Sprintf("item_%d", j)
			if err := twr.BloomFilterAdd(key, item); err != nil {
				b.Fatalf("Re-add BloomFilterAdd failed: %v", err)
			}
		}
	}
}

// Benchmark Bloom filter count
func BenchmarkBloomFilterCount(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:count:benchmark"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	// Setup data
	numItems := 1000
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("item_%d", i)
		if err := twr.BloomFilterAdd(key, item); err != nil {
			b.Fatalf("Setup BloomFilterAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.BloomFilterCount(key); err != nil {
			b.Fatalf("BloomFilterCount failed: %v", err)
		}
	}
}

// Benchmark Bloom filter delete
func BenchmarkDeleteBloomFilter(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bloom:delete:%d", i)
		if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
			b.Fatalf("CreateBloomFilter failed: %v", err)
		}

		// Add some items
		for j := 0; j < 10; j++ {
			item := fmt.Sprintf("item_%d", j)
			if err := twr.BloomFilterAdd(key, item); err != nil {
				b.Fatalf("BloomFilterAdd failed: %v", err)
			}
		}

		if err := twr.DeleteBloomFilter(key); err != nil {
			b.Fatalf("DeleteBloomFilter failed: %v", err)
		}
	}
}

// Benchmark concurrent Bloom filter operations
func BenchmarkConcurrentBloomFilterAdd(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:concurrent:add"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			item := fmt.Sprintf("item_%d", i)
			if err := twr.BloomFilterAdd(key, item); err != nil {
				b.Fatalf("Concurrent BloomFilterAdd failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentBloomFilterContains(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bloom:concurrent:contains"
	if err := twr.CreateBloomFilter(key, 0); err != nil { // 기본 슬롯 사용
		b.Fatalf("Setup failed: %v", err)
	}

	// Setup data
	numItems := 1000
	for i := 0; i < numItems; i++ {
		item := fmt.Sprintf("item_%d", i)
		if err := twr.BloomFilterAdd(key, item); err != nil {
			b.Fatalf("Setup BloomFilterAdd failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			item := fmt.Sprintf("item_%d", i%numItems)
			if _, err := twr.BloomFilterContains(key, item); err != nil {
				b.Fatalf("Concurrent BloomFilterContains failed: %v", err)
			}
			i++
		}
	})
}
