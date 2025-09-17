package tower

import (
	"testing"
	"time"
)

func TestTimeSeriesOperations(t *testing.T) {
	tower, err := NewTower(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    NewSizeFromMegabytes(64),
		MemTableSize: NewSizeFromMegabytes(16),
		BytesPerSync: NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test TimeSeriesCreate and TimeSeriesExists
	t.Run("create and exists", func(t *testing.T) {
		key := "test-timeseries-create"
		err := tower.TimeSeriesCreate(key)
		if err != nil {
			t.Errorf("TimeSeriesCreate failed: %v", err)
		}

		exists, err := tower.TimeSeriesExists(key)
		if err != nil {
			t.Errorf("TimeSeriesExists failed: %v", err)
		}
		if !exists {
			t.Errorf("Expected time series to exist")
		}
	})

	// Test TimeSeriesAdd and TimeSeriesGet
	t.Run("add and get", func(t *testing.T) {
		key := "test-timeseries-add"
		err := tower.TimeSeriesCreate(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		now := time.Now().UTC()
		intValue := PrimitiveInt(42)

		err = tower.TimeSeriesAdd(key, now, intValue)
		if err != nil {
			t.Errorf("TimeSeriesAdd failed: %v", err)
		}

		retrievedValue, err := tower.TimeSeriesGet(key, now)
		if err != nil {
			t.Errorf("TimeSeriesGet failed: %v", err)
		}

		retrievedInt, err := retrievedValue.Int()
		if err != nil {
			t.Errorf("Failed to get int value: %v", err)
		}

		if retrievedInt != 42 {
			t.Errorf("Expected 42, got %d", retrievedInt)
		}
	})

	// Test TimeSeriesRemove
	t.Run("remove", func(t *testing.T) {
		key := "test-timeseries-remove"
		err := tower.TimeSeriesCreate(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		now := time.Now().UTC()
		intValue := PrimitiveInt(42)

		err = tower.TimeSeriesAdd(key, now, intValue)
		if err != nil {
			t.Fatalf("Failed to add data point: %v", err)
		}

		err = tower.TimeSeriesRemove(key, now)
		if err != nil {
			t.Errorf("TimeSeriesRemove failed: %v", err)
		}

		_, err = tower.TimeSeriesGet(key, now)
		if err == nil {
			t.Errorf("Expected error when getting removed data point")
		}
	})

	// Test TimeSeriesRange
	t.Run("range", func(t *testing.T) {
		key := "test-timeseries-range"
		err := tower.TimeSeriesCreate(key)
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
			err = tower.TimeSeriesAdd(key, timestamp, values[i])
			if err != nil {
				t.Fatalf("Failed to add data point %d: %v", i, err)
			}
		}

		rangeData, err := tower.TimeSeriesRange(key, baseTime.Add(-2*time.Hour), baseTime.Add(2*time.Hour))
		if err != nil {
			t.Errorf("TimeSeriesRange failed: %v", err)
		}

		if len(rangeData) < 5 {
			t.Errorf("Expected at least 5 data points, got %d", len(rangeData))
		}
	})

	// Test DeleteTimeSeries
	t.Run("delete", func(t *testing.T) {
		key := "test-timeseries-delete"
		err := tower.TimeSeriesCreate(key)
		if err != nil {
			t.Fatalf("Failed to create time series: %v", err)
		}

		err = tower.DeleteTimeSeries(key)
		if err != nil {
			t.Errorf("DeleteTimeSeries failed: %v", err)
		}

		exists, err := tower.TimeSeriesExists(key)
		if err != nil {
			t.Errorf("TimeSeriesExists failed: %v", err)
		}
		if exists {
			t.Errorf("Time series should not exist after deletion")
		}
	})

	// Test with different types
	t.Run("different types", func(t *testing.T) {
		key := "test-mixed-types"
		err := tower.TimeSeriesCreate(key)
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
			err = tower.TimeSeriesAdd(key, tc.timestamp, tc.value)
			if err != nil {
				t.Errorf("Failed to add %s data point: %v", tc.typeName, err)
			}

			retrieved, err := tower.TimeSeriesGet(key, tc.timestamp)
			if err != nil {
				t.Errorf("Failed to get %s data point: %v", tc.typeName, err)
			}

			if retrieved.Type() != tc.value.Type() {
				t.Errorf("Type mismatch for %s: expected %v, got %v", tc.typeName, tc.value.Type(), retrieved.Type())
			}
		}

		rangeData, err := tower.TimeSeriesRange(key, now.Add(-5*time.Minute), now)
		if err != nil {
			t.Errorf("Failed to get range data: %v", err)
		}

		if len(rangeData) != len(testCases) {
			t.Errorf("Expected %d data points, got %d", len(testCases), len(rangeData))
		}
	})
}

func TestTimeSeriesWithDifferentTypes(t *testing.T) {
	// 테스트용 Tower 인스턴스 생성
	tower, err := NewTower(&Options{
		Path:         "test-timeseries-types.db",
		BytesPerSync: SizeKilobytes,
		CacheSize:    SizeMegabytes,
		MemTableSize: SizeMegabytes,
		FS:           InMemory(),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	key := "test-mixed-types"

	// TimeSeries 생성
	err = tower.TimeSeriesCreate(key)
	if err != nil {
		t.Fatalf("Failed to create time series: %v", err)
	}

	now := time.Now().UTC()

	// 다양한 타입의 데이터 추가
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
		err = tower.TimeSeriesAdd(key, tc.timestamp, tc.value)
		if err != nil {
			t.Fatalf("Failed to add %s data point: %v", tc.typeName, err)
		}

		// 즉시 조회해서 확인
		retrieved, err := tower.TimeSeriesGet(key, tc.timestamp)
		if err != nil {
			t.Fatalf("Failed to get %s data point: %v", tc.typeName, err)
		}

		if retrieved.Type() != tc.value.Type() {
			t.Fatalf("Type mismatch for %s: expected %v, got %v", tc.typeName, tc.value.Type(), retrieved.Type())
		}
	}

	// 범위 조회로 모든 데이터 확인
	rangeData, err := tower.TimeSeriesRange(key, now.Add(-5*time.Minute), now)
	if err != nil {
		t.Fatalf("Failed to get range data: %v", err)
	}

	if len(rangeData) != len(testCases) {
		t.Fatalf("Expected %d data points, got %d", len(testCases), len(rangeData))
	}
}
