package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"github.com/rivulet-io/tower/op"
)

func BenchmarkTimeSeriesCreate(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ts-create-%d", i)
		err := twr.TimeSeriesCreate(key)
		if err != nil {
			b.Fatalf("Failed to create time series: %v", err)
		}
	}
}

func BenchmarkTimeSeriesExists(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Pre-create time series
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("ts-exists-%d", i)
		twr.TimeSeriesCreate(key)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("ts-exists-%d", i%1000)
		_, err := twr.TimeSeriesExists(key)
		if err != nil {
			b.Fatalf("Failed to check time series existence: %v", err)
		}
	}
}

func BenchmarkTimeSeriesAdd(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Create time series
	key := "ts-add-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()
	values := []op.PrimitiveData{
		op.PrimitiveInt(42),
		op.PrimitiveFloat(3.14),
		op.PrimitiveString("test"),
		op.PrimitiveBool(true),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Nanosecond)
		value := values[i%len(values)]
		err := twr.TimeSeriesAdd(key, timestamp, value)
		if err != nil {
			b.Fatalf("Failed to add data point: %v", err)
		}
	}
}

func BenchmarkTimeSeriesGet(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	// Create time series and add data points
	key := "ts-get-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()
	timestamps := make([]time.Time, 1000)
	for i := 0; i < 1000; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Minute)
		timestamps[i] = timestamp
		err := twr.TimeSeriesAdd(key, timestamp, op.PrimitiveInt(int64(i)))
		if err != nil {
			b.Fatalf("Failed to add data point: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := timestamps[i%1000]
		_, err := twr.TimeSeriesGet(key, timestamp)
		if err != nil {
			b.Fatalf("Failed to get data point: %v", err)
		}
	}
}

func BenchmarkTimeSeriesIntOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "ts-int-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Nanosecond)
		value := op.PrimitiveInt(int64(i))
		err := twr.TimeSeriesAdd(key, timestamp, value)
		if err != nil {
			b.Fatalf("Failed to add int data point: %v", err)
		}
	}
}

func BenchmarkTimeSeriesFloatOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "ts-float-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Nanosecond)
		value := op.PrimitiveFloat(float64(i) * 3.14)
		err := twr.TimeSeriesAdd(key, timestamp, value)
		if err != nil {
			b.Fatalf("Failed to add float data point: %v", err)
		}
	}
}

func BenchmarkTimeSeriesStringOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "ts-string-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Nanosecond)
		value := op.PrimitiveString(fmt.Sprintf("value-%d", i))
		err := twr.TimeSeriesAdd(key, timestamp, value)
		if err != nil {
			b.Fatalf("Failed to add string data point: %v", err)
		}
	}
}

func BenchmarkTimeSeriesBoolOperations(b *testing.B) {
	twr := setupTowerForBenchmark(b)
	defer twr.Close()

	key := "ts-bool-bench"
	err := twr.TimeSeriesCreate(key)
	if err != nil {
		b.Fatalf("Failed to create time series: %v", err)
	}

	baseTime := time.Now().UTC()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		timestamp := baseTime.Add(time.Duration(i) * time.Nanosecond)
		value := op.PrimitiveBool(i%2 == 0)
		err := twr.TimeSeriesAdd(key, timestamp, value)
		if err != nil {
			b.Fatalf("Failed to add bool data point: %v", err)
		}
	}
}
