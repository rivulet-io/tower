package benchmarks

import (
	"fmt"
	"testing"
)

// Benchmark basic float operations
func BenchmarkSetFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("float:set:%d", i)
		if err := twr.SetFloat(key, float64(i)+0.5); err != nil {
			b.Fatalf("SetFloat failed: %v", err)
		}
	}
}

func BenchmarkGetFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "float:get:benchmark"
	if err := twr.SetFloat(key, 123.456); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.GetFloat(key); err != nil {
			b.Fatalf("GetFloat failed: %v", err)
		}
	}
}

// Benchmark arithmetic operations
func BenchmarkAddFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:add:benchmark"
	if err := twr.SetFloat(key, 0.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.AddFloat(key, 1.5); err != nil {
			b.Fatalf("AddFloat failed: %v", err)
		}
	}
}

func BenchmarkSubFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:sub:benchmark"
	if err := twr.SetFloat(key, float64(b.N)*1.5); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SubFloat(key, 1.5); err != nil {
			b.Fatalf("SubFloat failed: %v", err)
		}
	}
}

func BenchmarkMulFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:mul:benchmark"
	if err := twr.SetFloat(key, 2.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MulFloat(key, 1.1); err != nil {
			b.Fatalf("MulFloat failed: %v", err)
		}
		// Reset to prevent overflow
		if i%10 == 9 {
			if err := twr.SetFloat(key, 2.0); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

func BenchmarkDivFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:div:benchmark"
	if err := twr.SetFloat(key, 1000000.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.DivFloat(key, 1.1); err != nil {
			b.Fatalf("DivFloat failed: %v", err)
		}
		// Reset to prevent underflow
		if i%10 == 9 {
			if err := twr.SetFloat(key, 1000000.0); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}

// Benchmark unary operations
func BenchmarkNegFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:neg:benchmark"
	if err := twr.SetFloat(key, 123.456); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.NegFloat(key); err != nil {
			b.Fatalf("NegFloat failed: %v", err)
		}
	}
}

func BenchmarkAbsFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:abs:benchmark"
	if err := twr.SetFloat(key, -123.456); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.AbsFloat(key); err != nil {
			b.Fatalf("AbsFloat failed: %v", err)
		}
	}
}

func BenchmarkSwapFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:swap:benchmark"
	if err := twr.SetFloat(key, 123.456); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SwapFloat(key, float64(i)+0.5); err != nil {
			b.Fatalf("SwapFloat failed: %v", err)
		}
	}
}

// Benchmark comparison operations
func BenchmarkCompareFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:compare:benchmark"
	if err := twr.SetFloat(key, 123.456); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.CompareFloat(key, float64(i)+0.5); err != nil {
			b.Fatalf("CompareFloat failed: %v", err)
		}
	}
}

func BenchmarkSetFloatIfGreater(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:setifgreater:benchmark"
	if err := twr.SetFloat(key, 0.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetFloatIfGreater(key, float64(i)+0.5); err != nil {
			b.Fatalf("SetFloatIfGreater failed: %v", err)
		}
	}
}

func BenchmarkSetFloatIfLess(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:setifless:benchmark"
	if err := twr.SetFloat(key, float64(b.N)+0.5); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.SetFloatIfLess(key, float64(b.N-i)+0.5); err != nil {
			b.Fatalf("SetFloatIfLess failed: %v", err)
		}
	}
}

func BenchmarkMinFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:min:benchmark"
	if err := twr.SetFloat(key, 100.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MinFloat(key, float64(i%200)+0.5); err != nil {
			b.Fatalf("MinFloat failed: %v", err)
		}
	}
}

func BenchmarkMaxFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:max:benchmark"
	if err := twr.SetFloat(key, 0.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MaxFloat(key, float64(i)+0.5); err != nil {
			b.Fatalf("MaxFloat failed: %v", err)
		}
	}
}

func BenchmarkClampFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:clamp:benchmark"
	if err := twr.SetFloat(key, 50.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ClampFloat(key, 0.0, 100.0); err != nil {
			b.Fatalf("ClampFloat failed: %v", err)
		}
	}
}

// Benchmark concurrent float operations
func BenchmarkConcurrentSetFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("float:concurrent:set:%d", i)
			if err := twr.SetFloat(key, float64(i)+0.5); err != nil {
				b.Fatalf("ConcurrentSetFloat failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentAddFloat(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("float:concurrent:add:%d", i)
		if err := twr.SetFloat(key, 0.0); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("float:concurrent:add:%d", i%numKeys)
			if _, err := twr.AddFloat(key, 1.5); err != nil {
				b.Fatalf("ConcurrentAddFloat failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark chained float operations
func BenchmarkChainedFloatOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "float:chained:benchmark"
	if err := twr.SetFloat(key, 100.0); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Perform a chain of operations
		if _, err := twr.AddFloat(key, 10.5); err != nil {
			b.Fatalf("ChainedOperations AddFloat failed: %v", err)
		}
		if _, err := twr.MulFloat(key, 1.1); err != nil {
			b.Fatalf("ChainedOperations MulFloat failed: %v", err)
		}
		if _, err := twr.DivFloat(key, 2.0); err != nil {
			b.Fatalf("ChainedOperations DivFloat failed: %v", err)
		}
		if _, err := twr.SubFloat(key, 5.0); err != nil {
			b.Fatalf("ChainedOperations SubFloat failed: %v", err)
		}

		// Reset periodically to prevent extreme values
		if i%10 == 9 {
			if err := twr.SetFloat(key, 100.0); err != nil {
				b.Fatalf("Reset failed: %v", err)
			}
		}
	}
}