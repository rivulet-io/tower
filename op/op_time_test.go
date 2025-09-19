package op

import (
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

func TestTimeOperations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create in-memory tower: %v", err)
	}
	defer tower.Close()

	// Test SetTime and GetTime
	t.Run("SetTime_GetTime", func(t *testing.T) {
		key := "test:time"
		testTime := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)

		err := tower.SetTime(key, testTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		retrieved, err := tower.GetTime(key)
		if err != nil {
			t.Fatalf("Failed to get time: %v", err)
		}

		if !retrieved.Equal(testTime) {
			t.Errorf("Expected %v, got %v", testTime, retrieved)
		}
	})

	// Test AddDurationToTime
	t.Run("AddDurationToTime", func(t *testing.T) {
		key := "test:time:add"
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		duration := 2 * time.Hour

		err := tower.SetTime(key, baseTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		result, err := tower.AddDurationToTime(key, duration)
		if err != nil {
			t.Fatalf("Failed to add duration: %v", err)
		}

		expected := baseTime.Add(duration)
		if !result.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify the value was actually updated in storage
		stored, err := tower.GetTime(key)
		if err != nil {
			t.Fatalf("Failed to get stored time: %v", err)
		}
		if !stored.Equal(expected) {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}
	})

	// Test SubDurationFromTime
	t.Run("SubDurationFromTime", func(t *testing.T) {
		key := "test:time:sub"
		baseTime := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
		duration := 3 * time.Hour

		err := tower.SetTime(key, baseTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		result, err := tower.SubDurationFromTime(key, duration)
		if err != nil {
			t.Fatalf("Failed to subtract duration: %v", err)
		}

		expected := baseTime.Add(-duration)
		if !result.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test time comparison operations
	t.Run("TimeComparisons", func(t *testing.T) {
		key := "test:time:compare"
		testTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		earlierTime := testTime.Add(-1 * time.Hour)
		laterTime := testTime.Add(1 * time.Hour)

		err := tower.SetTime(key, testTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		// Test TimeBefore
		before, err := tower.TimeBefore(key, laterTime)
		if err != nil {
			t.Fatalf("Failed to check TimeBefore: %v", err)
		}
		if !before {
			t.Error("Expected time to be before later time")
		}

		before, err = tower.TimeBefore(key, earlierTime)
		if err != nil {
			t.Fatalf("Failed to check TimeBefore: %v", err)
		}
		if before {
			t.Error("Expected time not to be before earlier time")
		}

		// Test TimeAfter
		after, err := tower.TimeAfter(key, earlierTime)
		if err != nil {
			t.Fatalf("Failed to check TimeAfter: %v", err)
		}
		if !after {
			t.Error("Expected time to be after earlier time")
		}

		after, err = tower.TimeAfter(key, laterTime)
		if err != nil {
			t.Fatalf("Failed to check TimeAfter: %v", err)
		}
		if after {
			t.Error("Expected time not to be after later time")
		}

		// Test TimeEqual
		equal, err := tower.TimeEqual(key, testTime)
		if err != nil {
			t.Fatalf("Failed to check TimeEqual: %v", err)
		}
		if !equal {
			t.Error("Expected times to be equal")
		}

		equal, err = tower.TimeEqual(key, laterTime)
		if err != nil {
			t.Fatalf("Failed to check TimeEqual: %v", err)
		}
		if equal {
			t.Error("Expected times not to be equal")
		}
	})

	// Test TimeDiff
	t.Run("TimeDiff", func(t *testing.T) {
		key := "test:time:diff"
		baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		otherTime := baseTime.Add(2 * time.Hour)

		err := tower.SetTime(key, baseTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		diff, err := tower.TimeDiff(key, otherTime)
		if err != nil {
			t.Fatalf("Failed to get time diff: %v", err)
		}

		expected := baseTime.Sub(otherTime) // -2 hours
		if diff != expected {
			t.Errorf("Expected diff %v, got %v", expected, diff)
		}
	})

	// Test IsZeroTime
	t.Run("IsZeroTime", func(t *testing.T) {
		key := "test:time:zero"
		zeroTime := time.Time{}

		err := tower.SetTime(key, zeroTime)
		if err != nil {
			t.Fatalf("Failed to set zero time: %v", err)
		}

		isZero, err := tower.IsZeroTime(key)
		if err != nil {
			t.Fatalf("Failed to check IsZeroTime: %v", err)
		}
		if !isZero {
			t.Error("Expected time to be zero")
		}

		// Test with non-zero time
		nonZeroTime := time.Now()
		err = tower.SetTime(key, nonZeroTime)
		if err != nil {
			t.Fatalf("Failed to set non-zero time: %v", err)
		}

		isZero, err = tower.IsZeroTime(key)
		if err != nil {
			t.Fatalf("Failed to check IsZeroTime: %v", err)
		}
		if isZero {
			t.Error("Expected time not to be zero")
		}
	})

	// Test conditional time setting
	t.Run("ConditionalTimeSetting", func(t *testing.T) {
		key := "test:time:conditional"
		baseTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		greaterTime := baseTime.Add(1 * time.Hour)
		lesserTime := baseTime.Add(-1 * time.Hour)

		err := tower.SetTime(key, baseTime)
		if err != nil {
			t.Fatalf("Failed to set base time: %v", err)
		}

		// Test SetTimeIfGreater
		result, err := tower.SetTimeIfGreater(key, greaterTime)
		if err != nil {
			t.Fatalf("Failed to SetTimeIfGreater: %v", err)
		}
		if !result.Equal(greaterTime) {
			t.Errorf("Expected %v, got %v", greaterTime, result)
		}

		// Try setting with a lesser time (should not change)
		result, err = tower.SetTimeIfGreater(key, lesserTime)
		if err != nil {
			t.Fatalf("Failed to SetTimeIfGreater: %v", err)
		}
		if !result.Equal(greaterTime) {
			t.Errorf("Expected %v (unchanged), got %v", greaterTime, result)
		}

		// Test SetTimeIfLess
		result, err = tower.SetTimeIfLess(key, lesserTime)
		if err != nil {
			t.Fatalf("Failed to SetTimeIfLess: %v", err)
		}
		if !result.Equal(lesserTime) {
			t.Errorf("Expected %v, got %v", lesserTime, result)
		}

		// Test SetTimeIfEqual
		expected := lesserTime
		newTime := baseTime
		result, err = tower.SetTimeIfEqual(key, expected, newTime)
		if err != nil {
			t.Fatalf("Failed to SetTimeIfEqual: %v", err)
		}
		if !result.Equal(newTime) {
			t.Errorf("Expected %v, got %v", newTime, result)
		}
	})

	// Test time element extraction
	t.Run("TimeElementExtraction", func(t *testing.T) {
		key := "test:time:elements"
		testTime := time.Date(2024, 7, 15, 14, 30, 45, 123456789, time.UTC)

		err := tower.SetTime(key, testTime)
		if err != nil {
			t.Fatalf("Failed to set time: %v", err)
		}

		// Test GetTimeYear
		year, err := tower.GetTimeYear(key)
		if err != nil {
			t.Fatalf("Failed to get year: %v", err)
		}
		if year != 2024 {
			t.Errorf("Expected year 2024, got %d", year)
		}

		// Test GetTimeMonth
		month, err := tower.GetTimeMonth(key)
		if err != nil {
			t.Fatalf("Failed to get month: %v", err)
		}
		if month != time.July {
			t.Errorf("Expected month July, got %v", month)
		}

		// Test GetTimeDay
		day, err := tower.GetTimeDay(key)
		if err != nil {
			t.Fatalf("Failed to get day: %v", err)
		}
		if day != 15 {
			t.Errorf("Expected day 15, got %d", day)
		}

		// Test GetTimeHour
		hour, err := tower.GetTimeHour(key)
		if err != nil {
			t.Fatalf("Failed to get hour: %v", err)
		}
		if hour != 14 {
			t.Errorf("Expected hour 14, got %d", hour)
		}

		// Test GetTimeMinute
		minute, err := tower.GetTimeMinute(key)
		if err != nil {
			t.Fatalf("Failed to get minute: %v", err)
		}
		if minute != 30 {
			t.Errorf("Expected minute 30, got %d", minute)
		}

		// Test GetTimeSecond
		second, err := tower.GetTimeSecond(key)
		if err != nil {
			t.Fatalf("Failed to get second: %v", err)
		}
		if second != 45 {
			t.Errorf("Expected second 45, got %d", second)
		}

		// Test GetTimeNanosecond
		nanosecond, err := tower.GetTimeNanosecond(key)
		if err != nil {
			t.Fatalf("Failed to get nanosecond: %v", err)
		}
		if nanosecond != 123456789 {
			t.Errorf("Expected nanosecond 123456789, got %d", nanosecond)
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:time:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetTime(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.AddDurationToTime(nonExistentKey, time.Hour)
		if err == nil {
			t.Error("Expected error when adding duration to non-existent key")
		}

		_, err = tower.TimeBefore(nonExistentKey, time.Now())
		if err == nil {
			t.Error("Expected error when comparing non-existent key")
		}
	})
}
