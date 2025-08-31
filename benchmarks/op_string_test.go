package benchmarks

import (
	"fmt"
	"strings"
	"testing"
)

// Benchmark basic string operations
func BenchmarkSetString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("string:set:%d", i)
		value := fmt.Sprintf("test_value_%d", i)
		if err := twr.SetString(key, value); err != nil {
			b.Fatalf("SetString failed: %v", err)
		}
	}
}

func BenchmarkGetString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "string:get:benchmark"
	if err := twr.SetString(key, "Hello, World!"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.GetString(key); err != nil {
			b.Fatalf("GetString failed: %v", err)
		}
	}
}

// Benchmark string modification operations
func BenchmarkAppendString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:append:benchmark"
	if err := twr.SetString(key, "Hello"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		suffix := fmt.Sprintf("_%d", i%10) // Keep string manageable
		if _, err := twr.AppendString(key, suffix); err != nil {
			b.Fatalf("AppendString failed: %v", err)
		}
		
		// Reset periodically to prevent extremely long strings
		if i%100 == 99 {
			if err := twr.SetString(key, "Hello"); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkPrependString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:prepend:benchmark"
	if err := twr.SetString(key, "World"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prefix := fmt.Sprintf("%d_", i%10) // Keep string manageable
		if _, err := twr.PrependString(key, prefix); err != nil {
			b.Fatalf("PrependString failed: %v", err)
		}
		
		// Reset periodically to prevent extremely long strings
		if i%100 == 99 {
			if err := twr.SetString(key, "World"); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkReplaceString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:replace:benchmark"
	if err := twr.SetString(key, "Hello World Hello World"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ReplaceString(key, "World", "Go"); err != nil {
			b.Fatalf("ReplaceString failed: %v", err)
		}
		// Reset for next iteration
		if err := twr.SetString(key, "Hello World Hello World"); err != nil {
			b.Fatalf("Reset failed: %v", err)
		}
	}
}

// Benchmark string transformation operations
func BenchmarkUpperString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:upper:benchmark"
	if err := twr.SetString(key, "hello world this is a test string"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.UpperString(key); err != nil {
			b.Fatalf("UpperString failed: %v", err)
		}
	}
}

func BenchmarkLowerString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:lower:benchmark"
	if err := twr.SetString(key, "HELLO WORLD THIS IS A TEST STRING"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.LowerString(key); err != nil {
			b.Fatalf("LowerString failed: %v", err)
		}
	}
}

// Benchmark string query operations
func BenchmarkLengthString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:length:benchmark"
	if err := twr.SetString(key, "Hello, World! This is a test string."); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.LengthString(key); err != nil {
			b.Fatalf("LengthString failed: %v", err)
		}
	}
}

func BenchmarkContainsString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:contains:benchmark"
	if err := twr.SetString(key, "Hello, World! This is a test string."); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ContainsString(key, "World"); err != nil {
			b.Fatalf("ContainsString failed: %v", err)
		}
	}
}

func BenchmarkStartsWithString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:startswith:benchmark"
	if err := twr.SetString(key, "Hello, World! This is a test string."); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.StartsWithString(key, "Hello"); err != nil {
			b.Fatalf("StartsWithString failed: %v", err)
		}
	}
}

func BenchmarkEndsWithString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:endswith:benchmark"
	if err := twr.SetString(key, "Hello, World! This is a test string."); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.EndsWithString(key, "string."); err != nil {
			b.Fatalf("EndsWithString failed: %v", err)
		}
	}
}

func BenchmarkSubstringString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:substring:benchmark"
	if err := twr.SetString(key, "Hello, World! This is a test string."); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := i % 10
		end := start + 5
		if _, err := twr.SubstringString(key, start, end); err != nil {
			b.Fatalf("SubstringString failed: %v", err)
		}
	}
}

// Benchmark string comparison operations
func BenchmarkCompareString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:compare:benchmark"
	if err := twr.SetString(key, "Hello, World!"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compareWith := fmt.Sprintf("Hello, World! %d", i%10)
		if _, err := twr.CompareString(key, compareWith); err != nil {
			b.Fatalf("CompareString failed: %v", err)
		}
	}
}

func BenchmarkEqualString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:equal:benchmark"
	if err := twr.SetString(key, "Hello, World!"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.EqualString(key, "Hello, World!"); err != nil {
			b.Fatalf("EqualString failed: %v", err)
		}
	}
}

// Benchmark concurrent string operations
func BenchmarkConcurrentSetString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("string:concurrent:set:%d", i)
			value := fmt.Sprintf("concurrent_value_%d", i)
			if err := twr.SetString(key, value); err != nil {
				b.Fatalf("ConcurrentSetString failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentAppendString(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("string:concurrent:append:%d", i)
		if err := twr.SetString(key, "base"); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("string:concurrent:append:%d", i%numKeys)
			suffix := fmt.Sprintf("_%d", i)
			if _, err := twr.AppendString(key, suffix); err != nil {
				b.Fatalf("ConcurrentAppendString failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark chained string operations
func BenchmarkChainedStringOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "string:chained:benchmark"
	if err := twr.SetString(key, "hello world"); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Perform a chain of operations
		if _, err := twr.UpperString(key); err != nil {
			b.Fatalf("ChainedOperations UpperString failed: %v", err)
		}
		if _, err := twr.AppendString(key, "!"); err != nil {
			b.Fatalf("ChainedOperations AppendString failed: %v", err)
		}
		if _, err := twr.ReplaceString(key, "WORLD", "GO"); err != nil {
			b.Fatalf("ChainedOperations ReplaceString failed: %v", err)
		}

		// Reset for next iteration
		if err := twr.SetString(key, "hello world"); err != nil {
			b.Fatalf("Reset failed: %v", err)
		}
	}
}

// Benchmark string operations with different sizes
func BenchmarkStringOperationsSmall(b *testing.B) {
	benchmarkStringOperationsBySize(b, "small", strings.Repeat("a", 10))
}

func BenchmarkStringOperationsMedium(b *testing.B) {
	benchmarkStringOperationsBySize(b, "medium", strings.Repeat("a", 100))
}

func BenchmarkStringOperationsLarge(b *testing.B) {
	benchmarkStringOperationsBySize(b, "large", strings.Repeat("a", 1000))
}

func benchmarkStringOperationsBySize(b *testing.B, size, value string) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := fmt.Sprintf("string:size:%s", size)
	if err := twr.SetString(key, value); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 4 {
		case 0:
			if _, err := twr.GetString(key); err != nil {
				b.Fatalf("GetString failed: %v", err)
			}
		case 1:
			if _, err := twr.LengthString(key); err != nil {
				b.Fatalf("LengthString failed: %v", err)
			}
		case 2:
			if _, err := twr.UpperString(key); err != nil {
				b.Fatalf("UpperString failed: %v", err)
			}
		case 3:
			if _, err := twr.ContainsString(key, "a"); err != nil {
				b.Fatalf("ContainsString failed: %v", err)
			}
		}
	}
}