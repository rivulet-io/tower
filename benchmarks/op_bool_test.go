package benchmarks

import (
	"fmt"
	"testing"
)

// Benchmark basic boolean operations
func BenchmarkSetBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bool:set:%d", i)
		value := i%2 == 0 // Alternate between true and false
		if err := twr.SetBool(key, value); err != nil {
			b.Fatalf("SetBool failed: %v", err)
		}
	}
}

func BenchmarkGetBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup data
	key := "bool:get:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.GetBool(key); err != nil {
			b.Fatalf("GetBool failed: %v", err)
		}
	}
}

// Benchmark logical operations with single key and value
func BenchmarkAndBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:and:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := i%2 == 0
		if _, err := twr.AndBool(key, value); err != nil {
			b.Fatalf("AndBool failed: %v", err)
		}
	}
}

func BenchmarkOrBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:or:benchmark"
	if err := twr.SetBool(key, false); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := i%2 == 0
		if _, err := twr.OrBool(key, value); err != nil {
			b.Fatalf("OrBool failed: %v", err)
		}
	}
}

func BenchmarkXorBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:xor:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := i%2 == 0
		if _, err := twr.XorBool(key, value); err != nil {
			b.Fatalf("XorBool failed: %v", err)
		}
	}
}

// Benchmark single value operations
func BenchmarkNotBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:not:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.NotBool(key); err != nil {
			b.Fatalf("NotBool failed: %v", err)
		}
	}
}

func BenchmarkToggleBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:toggle:benchmark"
	if err := twr.SetBool(key, false); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.ToggleBool(key); err != nil {
			b.Fatalf("ToggleBool failed: %v", err)
		}
	}
}

// Benchmark comparison operations
func BenchmarkEqualBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:equal:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compareWith := i%2 == 0 // Alternate between true and false
		if _, err := twr.EqualBool(key, compareWith); err != nil {
			b.Fatalf("EqualBool failed: %v", err)
		}
	}
}

// Benchmark conditional operations
func BenchmarkSetBoolIfEqual(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "bool:setifequal:benchmark"
	if err := twr.SetBool(key, true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		expected := true
		newValue := i%2 == 0
		if _, err := twr.SetBoolIfEqual(key, expected, newValue); err != nil {
			b.Fatalf("SetBoolIfEqual failed: %v", err)
		}
		// Reset for next iteration
		if err := twr.SetBool(key, true); err != nil {
			b.Fatalf("Reset failed: %v", err)
		}
	}
}

// Benchmark concurrent boolean operations
func BenchmarkConcurrentSetBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bool:concurrent:set:%d", i)
			value := i%2 == 0
			if err := twr.SetBool(key, value); err != nil {
				b.Fatalf("ConcurrentSetBool failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentToggleBool(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bool:concurrent:toggle:%d", i)
		if err := twr.SetBool(key, false); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("bool:concurrent:toggle:%d", i%numKeys)
			if _, err := twr.ToggleBool(key); err != nil {
				b.Fatalf("ConcurrentToggleBool failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark mixed boolean operations
func BenchmarkMixedBoolOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup base keys
	numKeys := 10
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("bool:mixed:%d", i)
		if err := twr.SetBool(key, i%2 == 0); err != nil {
			b.Fatalf("Setup failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bool:mixed:%d", i%numKeys)
		
		switch i % 6 {
		case 0:
			if _, err := twr.GetBool(key); err != nil {
				b.Fatalf("MixedOperations GetBool failed: %v", err)
			}
		case 1:
			if err := twr.SetBool(key, i%2 == 0); err != nil {
				b.Fatalf("MixedOperations SetBool failed: %v", err)
			}
		case 2:
			if _, err := twr.ToggleBool(key); err != nil {
				b.Fatalf("MixedOperations ToggleBool failed: %v", err)
			}
		case 3:
			if _, err := twr.NotBool(key); err != nil {
				b.Fatalf("MixedOperations NotBool failed: %v", err)
			}
		case 4:
			if _, err := twr.EqualBool(key, true); err != nil {
				b.Fatalf("MixedOperations EqualBool failed: %v", err)
			}
		case 5:
			if _, err := twr.SetBoolIfEqual(key, true, false); err != nil {
				b.Fatalf("MixedOperations SetBoolIfEqual failed: %v", err)
			}
		}
	}
}

// Benchmark boolean operations with high contention
func BenchmarkHighContentionBoolOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Single key for high contention
	key := "bool:contention:shared"
	if err := twr.SetBool(key, false); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Each goroutine tries to toggle the same key
			if _, err := twr.ToggleBool(key); err != nil {
				b.Fatalf("HighContentionBoolOperations failed: %v", err)
			}
		}
	})
}

// Benchmark boolean state machine simulation
func BenchmarkBoolStateMachine(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup state machine keys
	stateKeys := map[string]string{
		"idle":    "bool:state:idle",
		"running": "bool:state:running",
		"paused":  "bool:state:paused",
		"stopped": "bool:state:stopped",
	}

	// Initialize state (idle = true, others = false)
	if err := twr.SetBool(stateKeys["idle"], true); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}
	for state, key := range stateKeys {
		if state != "idle" {
			if err := twr.SetBool(key, false); err != nil {
				b.Fatalf("Setup failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate state transitions
		switch i % 4 {
		case 0: // idle -> running
			if err := twr.SetBool(stateKeys["idle"], false); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["running"], true); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
		case 1: // running -> paused
			if err := twr.SetBool(stateKeys["running"], false); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["paused"], true); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
		case 2: // paused -> running
			if err := twr.SetBool(stateKeys["paused"], false); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["running"], true); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
		case 3: // running -> stopped -> idle
			if err := twr.SetBool(stateKeys["running"], false); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["stopped"], true); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["stopped"], false); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
			if err := twr.SetBool(stateKeys["idle"], true); err != nil {
				b.Fatalf("StateMachine transition failed: %v", err)
			}
		}
	}
}