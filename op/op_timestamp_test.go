package op

import (
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

func TestTimestampOperations(t *testing.T) {
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

	// Test SetTimestamp and GetTimestamp
	t.Run("SetTimestamp_GetTimestamp", func(t *testing.T) {
		key := "test:timestamp"
		testTimestamp := time.Date(2024, 3, 20, 15, 45, 30, 987654321, time.UTC)

		err := tower.SetTimestamp(key, testTimestamp)
		if err != nil {
			t.Fatalf("Failed to set timestamp: %v", err)
		}

		retrieved, err := tower.GetTimestamp(key)
		if err != nil {
			t.Fatalf("Failed to get timestamp: %v", err)
		}

		if !retrieved.Equal(testTimestamp) {
			t.Errorf("Expected %v, got %v", testTimestamp, retrieved)
		}
	})

	// Test AddDurationToTimestamp
	t.Run("AddDurationToTimestamp", func(t *testing.T) {
		key := "test:timestamp:add"
		baseTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		duration := 5*time.Hour + 30*time.Minute

		err := tower.SetTimestamp(key, baseTimestamp)
		if err != nil {
			t.Fatalf("Failed to set timestamp: %v", err)
		}

		result, err := tower.AddDurationToTimestamp(key, duration)
		if err != nil {
			t.Fatalf("Failed to add duration: %v", err)
		}

		expected := baseTimestamp.Add(duration)
		if !result.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify the value was actually updated in storage
		stored, err := tower.GetTimestamp(key)
		if err != nil {
			t.Fatalf("Failed to get stored timestamp: %v", err)
		}
		if !stored.Equal(expected) {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}
	})

	// Test SubDurationFromTimestamp
	t.Run("SubDurationFromTimestamp", func(t *testing.T) {
		key := "test:timestamp:sub"
		baseTimestamp := time.Date(2024, 12, 25, 18, 30, 0, 0, time.UTC)
		duration := 2*time.Hour + 15*time.Minute

		err := tower.SetTimestamp(key, baseTimestamp)
		if err != nil {
			t.Fatalf("Failed to set timestamp: %v", err)
		}

		result, err := tower.SubDurationFromTimestamp(key, duration)
		if err != nil {
			t.Fatalf("Failed to subtract duration: %v", err)
		}

		expected := baseTimestamp.Add(-duration)
		if !result.Equal(expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test CompareTimestamp
	t.Run("CompareTimestamp", func(t *testing.T) {
		key := "test:timestamp:compare"
		baseTimestamp := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		earlierTimestamp := baseTimestamp.Add(-1 * time.Hour)
		laterTimestamp := baseTimestamp.Add(1 * time.Hour)

		err := tower.SetTimestamp(key, baseTimestamp)
		if err != nil {
			t.Fatalf("Failed to set timestamp: %v", err)
		}

		// Compare with earlier timestamp (should return 1)
		result, err := tower.CompareTimestamp(key, earlierTimestamp)
		if err != nil {
			t.Fatalf("Failed to compare timestamp: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1 (later), got %d", result)
		}

		// Compare with later timestamp (should return -1)
		result, err = tower.CompareTimestamp(key, laterTimestamp)
		if err != nil {
			t.Fatalf("Failed to compare timestamp: %v", err)
		}
		if result != -1 {
			t.Errorf("Expected -1 (earlier), got %d", result)
		}

		// Compare with same timestamp (should return 0)
		result, err = tower.CompareTimestamp(key, baseTimestamp)
		if err != nil {
			t.Fatalf("Failed to compare timestamp: %v", err)
		}
		if result != 0 {
			t.Errorf("Expected 0 (equal), got %d", result)
		}
	})

	// Test conditional timestamp setting
	t.Run("ConditionalTimestampSetting", func(t *testing.T) {
		key := "test:timestamp:conditional"
		baseTimestamp := time.Date(2024, 8, 10, 9, 15, 0, 0, time.UTC)
		greaterTimestamp := baseTimestamp.Add(2 * time.Hour)
		lesserTimestamp := baseTimestamp.Add(-30 * time.Minute)

		err := tower.SetTimestamp(key, baseTimestamp)
		if err != nil {
			t.Fatalf("Failed to set base timestamp: %v", err)
		}

		// Test SetTimestampIfGreater
		result, err := tower.SetTimestampIfGreater(key, greaterTimestamp)
		if err != nil {
			t.Fatalf("Failed to SetTimestampIfGreater: %v", err)
		}
		if !result.Equal(greaterTimestamp) {
			t.Errorf("Expected %v, got %v", greaterTimestamp, result)
		}

		// Try setting with a lesser timestamp (should not change)
		result, err = tower.SetTimestampIfGreater(key, lesserTimestamp)
		if err != nil {
			t.Fatalf("Failed to SetTimestampIfGreater: %v", err)
		}
		if !result.Equal(greaterTimestamp) {
			t.Errorf("Expected %v (unchanged), got %v", greaterTimestamp, result)
		}

		// Test SetTimestampIfLess
		result, err = tower.SetTimestampIfLess(key, lesserTimestamp)
		if err != nil {
			t.Fatalf("Failed to SetTimestampIfLess: %v", err)
		}
		if !result.Equal(lesserTimestamp) {
			t.Errorf("Expected %v, got %v", lesserTimestamp, result)
		}

		// Test SetTimestampIfEqual
		expected := lesserTimestamp
		newTimestamp := baseTimestamp
		result, err = tower.SetTimestampIfEqual(key, expected, newTimestamp)
		if err != nil {
			t.Fatalf("Failed to SetTimestampIfEqual: %v", err)
		}
		if !result.Equal(newTimestamp) {
			t.Errorf("Expected %v, got %v", newTimestamp, result)
		}

		// Try with wrong expected value (should not change)
		wrongExpected := greaterTimestamp
		anotherNew := time.Now()
		result, err = tower.SetTimestampIfEqual(key, wrongExpected, anotherNew)
		if err != nil {
			t.Fatalf("Failed to SetTimestampIfEqual: %v", err)
		}
		if !result.Equal(newTimestamp) {
			t.Errorf("Expected %v (unchanged), got %v", newTimestamp, result)
		}
	})

	// Test with current time operations
	t.Run("CurrentTimeOperations", func(t *testing.T) {
		key := "test:timestamp:current"
		now := time.Now()

		err := tower.SetTimestamp(key, now)
		if err != nil {
			t.Fatalf("Failed to set current timestamp: %v", err)
		}

		// Add some duration and verify
		duration := 10 * time.Minute
		result, err := tower.AddDurationToTimestamp(key, duration)
		if err != nil {
			t.Fatalf("Failed to add duration: %v", err)
		}

		expectedTime := now.Add(duration)
		if !result.Equal(expectedTime) {
			t.Errorf("Expected %v, got %v", expectedTime, result)
		}

		// Test comparison with current time
		compareResult, err := tower.CompareTimestamp(key, now)
		if err != nil {
			t.Fatalf("Failed to compare with original time: %v", err)
		}
		if compareResult != 1 {
			t.Errorf("Expected 1 (later than original), got %d", compareResult)
		}
	})

	// Test Unix timestamp scenarios
	t.Run("UnixTimestampScenarios", func(t *testing.T) {
		key := "test:timestamp:unix"

		// Test with Unix epoch
		unixEpoch := time.Unix(0, 0).UTC()
		err := tower.SetTimestamp(key, unixEpoch)
		if err != nil {
			t.Fatalf("Failed to set Unix epoch: %v", err)
		}

		retrieved, err := tower.GetTimestamp(key)
		if err != nil {
			t.Fatalf("Failed to get Unix epoch: %v", err)
		}

		if !retrieved.Equal(unixEpoch) {
			t.Errorf("Expected %v, got %v", unixEpoch, retrieved)
		}

		// Test with a specific Unix timestamp
		specificUnix := time.Unix(1640995200, 0).UTC() // 2022-01-01 00:00:00 UTC
		err = tower.SetTimestamp(key, specificUnix)
		if err != nil {
			t.Fatalf("Failed to set specific Unix timestamp: %v", err)
		}

		retrieved, err = tower.GetTimestamp(key)
		if err != nil {
			t.Fatalf("Failed to get specific Unix timestamp: %v", err)
		}

		if !retrieved.Equal(specificUnix) {
			t.Errorf("Expected %v, got %v", specificUnix, retrieved)
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:timestamp:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetTimestamp(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.AddDurationToTimestamp(nonExistentKey, time.Hour)
		if err == nil {
			t.Error("Expected error when adding duration to non-existent key")
		}

		_, err = tower.CompareTimestamp(nonExistentKey, time.Now())
		if err == nil {
			t.Error("Expected error when comparing non-existent key")
		}

		_, err = tower.SetTimestampIfGreater(nonExistentKey, time.Now())
		if err == nil {
			t.Error("Expected error when conditionally setting non-existent key")
		}
	})

	// Test timezone handling
	t.Run("TimezoneHandling", func(t *testing.T) {
		key := "test:timestamp:timezone"

		// Test with different timezones
		utcTime := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
		estLocation, _ := time.LoadLocation("America/New_York")
		estTime := time.Date(2024, 6, 15, 8, 0, 0, 0, estLocation) // Same moment as UTC

		err := tower.SetTimestamp(key, utcTime)
		if err != nil {
			t.Fatalf("Failed to set UTC timestamp: %v", err)
		}

		// Compare with EST time (should be equal in terms of the moment in time)
		compareResult, err := tower.CompareTimestamp(key, estTime)
		if err != nil {
			t.Fatalf("Failed to compare with EST time: %v", err)
		}
		if compareResult != 0 {
			t.Errorf("Expected 0 (equal moments), got %d", compareResult)
		}
	})
}
