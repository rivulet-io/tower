package op

import (
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

func TestTimeSeriesOperations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test CreateTimeSeries and ExistsTimeSeries
	t.Run("create and exists", func(t *testing.T) {
		key := "test-timeseries-create"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Errorf("CreateTimeSeries failed: %v", err)
		}

		exists, err := tower.ExistsTimeSeries(key)
		if err != nil {
			t.Errorf("ExistsTimeSeries failed: %v", err)
		}
		if !exists {
			t.Errorf("Expected time series to exist")
		}
	})

	// Test AddTimeSeriesPoint and GetTimeSeriesPoint
	t.Run("add and get", func(t *testing.T) {
		key := "test-timeseries-add"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		now := time.Now().UTC()
		intValue := PrimitiveInt(42)

		err = tower.AddTimeSeriesPoint(key, now, intValue)
		if err != nil {
			t.Errorf("AddTimeSeriesPoint failed: %v", err)
		}

		retrievedValue, err := tower.GetTimeSeriesPoint(key, now)
		if err != nil {
			t.Errorf("GetTimeSeriesPoint failed: %v", err)
		}

		retrievedInt, err := retrievedValue.Int()
		if err != nil {
			t.Errorf("Failed to get int value: %v", err)
		}

		if retrievedInt != 42 {
			t.Errorf("Expected 42, got %d", retrievedInt)
		}
	})

	// Test DeleteTimeSeriesPoint
	t.Run("remove", func(t *testing.T) {
		key := "test-timeseries-remove"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		now := time.Now().UTC()
		intValue := PrimitiveInt(42)

		err = tower.AddTimeSeriesPoint(key, now, intValue)
		if err != nil {
			t.Fatalf("Failed to add data point: %v", err)
		}

		err = tower.DeleteTimeSeriesPoint(key, now)
		if err != nil {
			t.Errorf("DeleteTimeSeriesPoint failed: %v", err)
		}

		_, err = tower.GetTimeSeriesPoint(key, now)
		if err == nil {
			t.Errorf("Expected error when getting removed data point")
		}
	})

	// Test GetTimeSeriesRange
	t.Run("range", func(t *testing.T) {
		key := "test-timeseries-range"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		baseTime := time.Now().UTC()
		times := []time.Time{
			baseTime.Add(-time.Hour),
			baseTime.Add(-30 * time.Minute),
			baseTime,
			baseTime.Add(30 * time.Minute),
			baseTime.Add(time.Hour),
		}
		values := []PrimitiveData{
			PrimitiveInt(10),
			PrimitiveInt(20),
			PrimitiveInt(42),
			PrimitiveInt(30),
			PrimitiveInt(40),
		}

		for i, timestamp := range times {
			err = tower.AddTimeSeriesPoint(key, timestamp, values[i])
			if err != nil {
				t.Fatalf("Failed to add data point %d: %v", i, err)
			}
		}

		rangeData, err := tower.GetTimeSeriesRange(key, baseTime.Add(-2*time.Hour), baseTime.Add(2*time.Hour))
		if err != nil {
			t.Errorf("GetTimeSeriesRange failed: %v", err)
		}

		if len(rangeData) < 5 {
			t.Errorf("Expected at least 5 data points, got %d", len(rangeData))
		}
	})

	// Test DeleteTimeSeries
	t.Run("delete", func(t *testing.T) {
		key := "test-timeseries-delete"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		err = tower.DeleteTimeSeries(key)
		if err != nil {
			t.Errorf("DeleteTimeSeries failed: %v", err)
		}

		exists, err := tower.ExistsTimeSeries(key)
		if err != nil {
			t.Errorf("ExistsTimeSeries failed: %v", err)
		}
		if exists {
			t.Errorf("Time series should not exist after deletion")
		}
	})

	// Test with different types
	t.Run("different types", func(t *testing.T) {
		key := "test-mixed-types"
		err := tower.CreateTimeSeries(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		now := time.Now().UTC()

		testCases := []struct {
			timestamp time.Time
			value     PrimitiveData
			typeName  string
		}{
			{now.Add(-4 * time.Minute), PrimitiveInt(100), "int"},
			{now.Add(-3 * time.Minute), PrimitiveFloat(3.14), "float"},
			{now.Add(-2 * time.Minute), PrimitiveString("hello"), "string"},
			{now.Add(-1 * time.Minute), PrimitiveBool(true), "bool"},
		}

		for _, tc := range testCases {
			err = tower.AddTimeSeriesPoint(key, tc.timestamp, tc.value)
			if err != nil {
				t.Errorf("Failed to add %s data point: %v", tc.typeName, err)
			}

			retrieved, err := tower.GetTimeSeriesPoint(key, tc.timestamp)
			if err != nil {
				t.Errorf("Failed to get %s data point: %v", tc.typeName, err)
			}

			if retrieved.Type() != tc.value.Type() {
				t.Errorf("Type mismatch for %s: expected %v, got %v", tc.typeName, tc.value.Type(), retrieved.Type())
			}
		}

		rangeData, err := tower.GetTimeSeriesRange(key, now.Add(-5*time.Minute), now)
		if err != nil {
			t.Errorf("Failed to get range data: %v", err)
		}

		if len(rangeData) != len(testCases) {
			t.Errorf("Expected %d data points, got %d", len(testCases), len(rangeData))
		}
	})
}

func TestTimeSeriesWithDifferentTypes(t *testing.T) {
	// Create Operator instance for testing
	tower, err := NewOperator(&Options{
		Path:         "test-timeseries-types.db",
		BytesPerSync: size.SizeKilobytes,
		CacheSize:    size.SizeMegabytes,
		MemTableSize: size.SizeMegabytes,
		FS:           InMemory(),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	key := "test-mixed-types"

	// Create TimeSeries
	err = tower.CreateTimeSeries(key)
	if err != nil {
		t.Fatalf("Failed to create time series: %v", err)
	}

	now := time.Now().UTC()

	// Add various types of data
	testCases := []struct {
		timestamp time.Time
		value     PrimitiveData
		typeName  string
	}{
		{now.Add(-4 * time.Minute), PrimitiveInt(100), "int"},
		{now.Add(-3 * time.Minute), PrimitiveFloat(3.14), "float"},
		{now.Add(-2 * time.Minute), PrimitiveString("hello"), "string"},
		{now.Add(-1 * time.Minute), PrimitiveBool(true), "bool"},
	}

	for _, tc := range testCases {
		err = tower.AddTimeSeriesPoint(key, tc.timestamp, tc.value)
		if err != nil {
			t.Fatalf("Failed to add %s data point: %v", tc.typeName, err)
		}

		// Query immediately to check
		retrieved, err := tower.GetTimeSeriesPoint(key, tc.timestamp)
		if err != nil {
			t.Fatalf("Failed to get %s data point: %v", tc.typeName, err)
		}

		if retrieved.Type() != tc.value.Type() {
			t.Fatalf("Type mismatch for %s: expected %v, got %v", tc.typeName, tc.value.Type(), retrieved.Type())
		}
	}

	// Check all data with range query
	rangeData, err := tower.GetTimeSeriesRange(key, now.Add(-5*time.Minute), now)
	if err != nil {
		t.Fatalf("Failed to get range data: %v", err)
	}

	if len(rangeData) != len(testCases) {
		t.Fatalf("Expected %d data points, got %d", len(testCases), len(rangeData))
	}
}




