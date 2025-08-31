package benchmarks

import (
	"fmt"
	"testing"

	"github.com/rivulet-io/tower"
)

// Benchmark basic map operations
func BenchmarkCreateMap(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("map:create:%d", i)
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("CreateMap failed: %v", err)
		}
	}
}

func BenchmarkDeleteMap(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Setup maps
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("map:delete:%d", i)
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("Setup CreateMap failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("map:delete:%d", i)
		if err := twr.DeleteMap(key); err != nil {
			b.Fatalf("DeleteMap failed: %v", err)
		}
	}
}

// Benchmark map field operations
func BenchmarkMapSet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:set:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet failed: %v", err)
		}
	}
}

func BenchmarkMapGet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:get:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate map
	for i := 0; i < 100; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i%100))
		if _, err := twr.MapGet(key, field); err != nil {
			b.Fatalf("MapGet failed: %v", err)
		}
	}
}

func BenchmarkMapDelete(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:delete:benchmark"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create map and add field for each iteration
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("CreateMap failed: %v", err)
		}

		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}

		// Delete the field
		if _, err := twr.MapDelete(key, field); err != nil {
			b.Fatalf("MapDelete failed: %v", err)
		}

		// Cleanup
		if err := twr.DeleteMap(key); err != nil {
			b.Fatalf("Cleanup failed: %v", err)
		}
	}
}

// Benchmark map query operations
func BenchmarkMapKeys(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:keys:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate map
	for i := 0; i < 100; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MapKeys(key); err != nil {
			b.Fatalf("MapKeys failed: %v", err)
		}
	}
}

func BenchmarkMapValues(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:values:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate map
	for i := 0; i < 100; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MapValues(key); err != nil {
			b.Fatalf("MapValues failed: %v", err)
		}
	}
}

func BenchmarkMapLength(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:length:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate map
	for i := 0; i < 100; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := twr.MapLength(key); err != nil {
			b.Fatalf("MapLength failed: %v", err)
		}
	}
}

func BenchmarkClearMap(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:clear:benchmark"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create and populate map for each iteration
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("CreateMap failed: %v", err)
		}

		for j := 0; j < 10; j++ {
			field := tower.PrimitiveString(fmt.Sprintf("field_%d", j))
			value := tower.PrimitiveInt(int64(j))
			if err := twr.MapSet(key, field, value); err != nil {
				b.Fatalf("Setup MapSet failed: %v", err)
			}
		}

		// Clear the map
		if err := twr.ClearMap(key); err != nil {
			b.Fatalf("ClearMap failed: %v", err)
		}

		// Cleanup
		if err := twr.DeleteMap(key); err != nil {
			b.Fatalf("Cleanup failed: %v", err)
		}
	}
}

// Benchmark map operations with different data types
func BenchmarkMapWithStrings(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:strings:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("string_field_%d", i))
		value := tower.PrimitiveString(fmt.Sprintf("string_value_%d", i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet with string failed: %v", err)
		}
	}
}

func BenchmarkMapWithInts(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:ints:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("int_field_%d", i))
		value := tower.PrimitiveInt(int64(i * 10))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet with int failed: %v", err)
		}
	}
}

func BenchmarkMapWithFloats(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:floats:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("float_field_%d", i))
		value := tower.PrimitiveFloat(float64(i) + 0.5)
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet with float failed: %v", err)
		}
	}
}

func BenchmarkMapWithBools(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:bools:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("bool_field_%d", i))
		value := tower.PrimitiveBool(i%3 == 0)
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet with bool failed: %v", err)
		}
	}
}

// Benchmark map operations with mixed data types
func BenchmarkMapMixedTypes(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:mixed:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("mixed_field_%d", i))
		
		var value tower.PrimitiveData
		switch i % 4 {
		case 0:
			value = tower.PrimitiveString(fmt.Sprintf("string_value_%d", i))
		case 1:
			value = tower.PrimitiveInt(int64(i))
		case 2:
			value = tower.PrimitiveFloat(float64(i) + 0.5)
		case 3:
			value = tower.PrimitiveBool(i%2 == 0)
		}

		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("MapSet with mixed types failed: %v", err)
		}
	}
}

// Benchmark concurrent map operations
func BenchmarkConcurrentMapSet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numMaps := 100
	for i := 0; i < numMaps; i++ {
		key := fmt.Sprintf("map:concurrent:set:%d", i)
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("Setup CreateMap failed: %v", err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("map:concurrent:set:%d", i%numMaps)
			field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
			value := tower.PrimitiveInt(int64(i))
			if err := twr.MapSet(key, field, value); err != nil {
				b.Fatalf("ConcurrentMapSet failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkConcurrentMapGet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	numMaps := 10
	fieldsPerMap := 100

	// Setup maps with data
	for i := 0; i < numMaps; i++ {
		key := fmt.Sprintf("map:concurrent:get:%d", i)
		if err := twr.CreateMap(key); err != nil {
			b.Fatalf("Setup CreateMap failed: %v", err)
		}

		for j := 0; j < fieldsPerMap; j++ {
			field := tower.PrimitiveString(fmt.Sprintf("field_%d", j))
			value := tower.PrimitiveInt(int64(j))
			if err := twr.MapSet(key, field, value); err != nil {
				b.Fatalf("Setup MapSet failed: %v", err)
			}
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("map:concurrent:get:%d", i%numMaps)
			field := tower.PrimitiveString(fmt.Sprintf("field_%d", i%fieldsPerMap))
			if _, err := twr.MapGet(key, field); err != nil {
				b.Fatalf("ConcurrentMapGet failed: %v", err)
			}
			i++
		}
	})
}

// Benchmark map operations by size
func BenchmarkSmallMapOperations(b *testing.B) {
	benchmarkMapOperationsBySize(b, "small", 10)
}

func BenchmarkMediumMapOperations(b *testing.B) {
	benchmarkMapOperationsBySize(b, "medium", 100)
}

func BenchmarkLargeMapOperations(b *testing.B) {
	benchmarkMapOperationsBySize(b, "large", 1000)
}

func benchmarkMapOperationsBySize(b *testing.B, size string, mapSize int) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := fmt.Sprintf("map:size:%s", size)
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate map
	for i := 0; i < mapSize; i++ {
		field := tower.PrimitiveString(fmt.Sprintf("field_%d", i))
		value := tower.PrimitiveInt(int64(i))
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup MapSet failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 5 {
		case 0:
			if _, err := twr.MapLength(key); err != nil {
				b.Fatalf("MapLength failed: %v", err)
			}
		case 1:
			field := tower.PrimitiveString(fmt.Sprintf("field_%d", i%mapSize))
			if _, err := twr.MapGet(key, field); err != nil {
				b.Fatalf("MapGet failed: %v", err)
			}
		case 2:
			field := tower.PrimitiveString(fmt.Sprintf("new_field_%d", i))
			value := tower.PrimitiveInt(int64(i))
			if err := twr.MapSet(key, field, value); err != nil {
				b.Fatalf("MapSet failed: %v", err)
			}
		case 3:
			if _, err := twr.MapKeys(key); err != nil {
				b.Fatalf("MapKeys failed: %v", err)
			}
		case 4:
			if _, err := twr.MapValues(key); err != nil {
				b.Fatalf("MapValues failed: %v", err)
			}
		}
	}
}

// Benchmark map as cache
func BenchmarkMapAsCache(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:cache:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cacheKey := tower.PrimitiveString(fmt.Sprintf("cache_key_%d", i%100)) // Simulate cache with 100 keys
		
		// 70% reads, 30% writes (typical cache pattern)
		if i%10 < 7 {
			// Read operation
			if _, err := twr.MapGet(key, cacheKey); err != nil {
				// Cache miss is normal, don't fail
			}
		} else {
			// Write operation
			value := tower.PrimitiveString(fmt.Sprintf("cache_value_%d", i))
			if err := twr.MapSet(key, cacheKey, value); err != nil {
				b.Fatalf("Cache MapSet failed: %v", err)
			}
		}
	}
}

// Benchmark map as configuration store
func BenchmarkMapAsConfig(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "map:config:benchmark"
	if err := twr.CreateMap(key); err != nil {
		b.Fatalf("Setup failed: %v", err)
	}

	// Pre-populate with typical config keys
	configs := map[tower.PrimitiveData]tower.PrimitiveData{
		tower.PrimitiveString("timeout"):     tower.PrimitiveInt(30),
		tower.PrimitiveString("max_retries"): tower.PrimitiveInt(3),
		tower.PrimitiveString("debug_mode"):  tower.PrimitiveBool(false),
		tower.PrimitiveString("app_name"):    tower.PrimitiveString("tower_app"),
		tower.PrimitiveString("version"):     tower.PrimitiveString("1.0.0"),
		tower.PrimitiveString("rate_limit"):  tower.PrimitiveFloat(100.5),
	}

	for field, value := range configs {
		if err := twr.MapSet(key, field, value); err != nil {
			b.Fatalf("Setup config failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mostly read operations for config
		configKeys := []tower.PrimitiveData{
			tower.PrimitiveString("timeout"),
			tower.PrimitiveString("max_retries"),
			tower.PrimitiveString("debug_mode"),
			tower.PrimitiveString("app_name"),
			tower.PrimitiveString("version"),
			tower.PrimitiveString("rate_limit"),
		}
		configKey := configKeys[i%len(configKeys)]
		
		if _, err := twr.MapGet(key, configKey); err != nil {
			b.Fatalf("Config MapGet failed: %v", err)
		}
	}
}