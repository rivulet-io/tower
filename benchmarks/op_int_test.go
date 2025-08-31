package benchmarks

import (
	"fmt"
	"testing"

	"github.com/rivulet-io/tower"
)

// setupTowerForBenchmark creates an in-memory tower for benchmarking
func setupTowerForBenchmark(b *testing.B) *tower.Tower {
	b.Helper()
	twr, err := tower.NewTower(&tower.Options{
		Path:         "benchmark_data",
		FS:           tower.InMemory(),
		CacheSize:    tower.NewSizeFromMegabytes(64),
		MemTableSize: tower.NewSizeFromMegabytes(16),
		BytesPerSync: tower.NewSizeFromKilobytes(512),
	})
	if err != nil {
		b.Fatalf("Failed to create tower: %v", err)
	}
	return twr
}

// Benchmark basic int operations
func BenchmarkSetInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("int:set:%d", i)
		if err := twr.SetInt(key, int64(i)); err != nil {
			b.Fatalf("SetInt failed: %v", err)
		}
	}
}

func BenchmarkGetInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "int:get:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.GetInt(key); err != nil {
			b.Fatalf("GetInt failed: %v", err)
		}
	}
}

// Benchmark arithmetic operations
func BenchmarkAddInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:add:benchmark"
	if err := twr.SetInt(key, 0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.AddInt(key, 1); err != nil {
			b.Fatalf("AddInt failed: %v", err)
		}
	}
}

func BenchmarkSubInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:sub:benchmark"
	if err := twr.SetInt(key, int64(b.N)); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SubInt(key, 1); err != nil {
			b.Fatalf("SubInt failed: %v", err)
		}
	}
}

func BenchmarkIncInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:inc:benchmark"
	if err := twr.SetInt(key, 0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.IncInt(key); err != nil {
			b.Fatalf("IncInt failed: %v", err)
		}
	}
}

func BenchmarkDecInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:dec:benchmark"
	if err := twr.SetInt(key, int64(b.N)); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.DecInt(key); err != nil {
			b.Fatalf("DecInt failed: %v", err)
		}
	}
}

func BenchmarkMulInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:mul:benchmark"
	if err := twr.SetInt(key, 2); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MulInt(key, 2); err != nil {
			b.Fatalf("MulInt failed: %v", err)
		}
		// Reset to prevent overflow
		if i%10 == 9 {
			if err := twr.SetInt(key, 2); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkDivInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:div:benchmark"
	if err := twr.SetInt(key, 1000000); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.DivInt(key, 2); err != nil {
			b.Fatalf("DivInt failed: %v", err)
		}
		// Reset to prevent underflow
		if i%10 == 9 {
			if err := twr.SetInt(key, 1000000); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkModInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:mod:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ModInt(key, 100); err != nil {
			b.Fatalf("ModInt failed: %v", err)
		}
	}
}

// Benchmark unary operations
func BenchmarkNegInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:neg:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.NegInt(key); err != nil {
			b.Fatalf("NegInt failed: %v", err)
		}
	}
}

func BenchmarkAbsInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:abs:benchmark"
	if err := twr.SetInt(key, -12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.AbsInt(key); err != nil {
			b.Fatalf("AbsInt failed: %v", err)
		}
	}
}

func BenchmarkSwapInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:swap:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SwapInt(key, int64(i)); err != nil {
			b.Fatalf("SwapInt failed: %v", err)
		}
	}
}

// Benchmark comparison operations
func BenchmarkCompareInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:compare:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.CompareInt(key, int64(i)); err != nil {
			b.Fatalf("CompareInt failed: %v", err)
		}
	}
}

func BenchmarkSetIntIfGreater(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:setifgreater:benchmark"
	if err := twr.SetInt(key, 0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetIntIfGreater(key, int64(i)); err != nil {
			b.Fatalf("SetIntIfGreater failed: %v", err)
		}
	}
}

func BenchmarkSetIntIfLess(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:setifless:benchmark"
	if err := twr.SetInt(key, int64(b.N)); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetIntIfLess(key, int64(b.N-i)); err != nil {
			b.Fatalf("SetIntIfLess failed: %v", err)
		}
	}
}

func BenchmarkSetIntIfEqual(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:setifequal:benchmark"
	if err := twr.SetInt(key, 12345); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expected := int64(12345)
		newValue := int64(i)
		if _, err := twr.SetIntIfEqual(key, expected, newValue); err != nil {
			b.Fatalf("SetIntIfEqual failed: %v", err)
		}
		// Reset for next iteration
		if err := twr.SetInt(key, 12345); err != nil {
			b.Fatalf("Reset failed: %v", err)
		}
	}
}

// Benchmark utility operations
func BenchmarkClampInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:clamp:benchmark"
	if err := twr.SetInt(key, 50); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ClampInt(key, 0, 100); err != nil {
			b.Fatalf("ClampInt failed: %v", err)
		}
	}
}

func BenchmarkMinInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:min:benchmark"
	if err := twr.SetInt(key, 100); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MinInt(key, int64(i%200)); err != nil {
			b.Fatalf("MinInt failed: %v", err)
		}
	}
}

func BenchmarkMaxInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:max:benchmark"
	if err := twr.SetInt(key, 0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MaxInt(key, int64(i)); err != nil {
			b.Fatalf("MaxInt failed: %v", err)
		}
	}
}

// Benchmark bitwise operations
func BenchmarkAndInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:and:benchmark"
	if err := twr.SetInt(key, 0xFF); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask := int64(0xF0 | (i & 0x0F))
		if _, err := twr.AndInt(key, mask); err != nil {
			b.Fatalf("AndInt failed: %v", err)
		}
	}
}

func BenchmarkOrInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:or:benchmark"
	if err := twr.SetInt(key, 0x00); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask := int64(i & 0xFF)
		if _, err := twr.OrInt(key, mask); err != nil {
			b.Fatalf("OrInt failed: %v", err)
		}
	}
}

func BenchmarkXorInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:xor:benchmark"
	if err := twr.SetInt(key, 0x55); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mask := int64(0xAA)
		if _, err := twr.XorInt(key, mask); err != nil {
			b.Fatalf("XorInt failed: %v", err)
		}
	}
}

func BenchmarkShiftLeftInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:shiftleft:benchmark"
	if err := twr.SetInt(key, 1); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bits := uint(i % 32) // Prevent overflow
		if _, err := twr.ShiftLeftInt(key, bits); err != nil {
			b.Fatalf("ShiftLeftInt failed: %v", err)
		}
		// Reset periodically to prevent overflow
		if i%10 == 9 {
			if err := twr.SetInt(key, 1); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkShiftRightInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:shiftright:benchmark"
	if err := twr.SetInt(key, 1<<30); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bits := uint(i%10 + 1) // Prevent underflow
		if _, err := twr.ShiftRightInt(key, bits); err != nil {
			b.Fatalf("ShiftRightInt failed: %v", err)
		}
		// Reset periodically to prevent underflow
		if i%10 == 9 {
			if err := twr.SetInt(key, 1<<30); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

// Benchmark concurrent operations on different keys
func BenchmarkConcurrentSetInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("int:concurrent:set:%d", i)
			if err := twr.SetInt(key, int64(i)); err != nil {
				b.Fatalf("ConcurrentSetInt failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentIncInt(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("int:concurrent:inc:%d", i)
		if err := twr.SetInt(key, 0); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("int:concurrent:inc:%d", i%numKeys)
			if _, err := twr.IncInt(key); err != nil {
				b.Fatalf("ConcurrentIncInt failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark chained operations
func BenchmarkChainedIntOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "int:chained:benchmark"
	if err := twr.SetInt(key, 100); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Perform a chain of operations
		if _, err := twr.AddInt(key, 10); err != nil {
			b.Fatalf("ChainedOperations AddInt failed: %v", err)
		}
		if _, err := twr.MulInt(key, 2); err != nil {
			b.Fatalf("ChainedOperations MulInt failed: %v", err)
		}
		if _, err := twr.DivInt(key, 3); err != nil {
			b.Fatalf("ChainedOperations DivInt failed: %v", err)
		}
		if _, err := twr.SubInt(key, 5); err != nil {
			b.Fatalf("ChainedOperations SubInt failed: %v", err)
		}

		// Reset periodically to prevent extreme values
		if i%10 == 9 {
			if err := twr.SetInt(key, 100); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

// Benchmark batch operations
func BenchmarkBatchIntOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	batchSize := 100
	keys := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		keys[i] = fmt.Sprintf("int:batch:%d", i)
		if err := twr.SetInt(keys[i], int64(i)); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			if _, err := twr.IncInt(keys[j]); err != nil {
				b.Fatalf("BatchIntOperations failed: %v", err)
			}
		}
	}
}
