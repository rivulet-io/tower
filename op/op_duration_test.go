package op

import (
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

func TestDurationOperations(t *testing.T) {
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

	// Test SetDuration and GetDuration
	t.Run("SetDuration_GetDuration", func(t *testing.T) {
		key := "test:duration"
		testDuration := 2*time.Hour + 30*time.Minute + 45*time.Second

		err := tower.SetDuration(key, testDuration)
		if err != nil {
			t.Fatalf("Failed to set duration: %v", err)
		}

		retrieved, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get duration: %v", err)
		}

		if retrieved != testDuration {
			t.Errorf("Expected %v, got %v", testDuration, retrieved)
		}
	})

	// Test duration arithmetic operations
	t.Run("DurationArithmetic", func(t *testing.T) {
		key := "test:duration:arithmetic"
		initialDuration := 1 * time.Hour

		err := tower.SetDuration(key, initialDuration)
		if err != nil {
			t.Fatalf("Failed to set initial duration: %v", err)
		}

		// Test AddDuration
		delta := 30 * time.Minute
		result, err := tower.AddDuration(key, delta)
		if err != nil {
			t.Fatalf("Failed to add duration: %v", err)
		}

		expected := initialDuration + delta
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify stored value
		stored, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get stored duration: %v", err)
		}
		if stored != expected {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}

		// Test SubDuration
		subDelta := 45 * time.Minute
		result, err = tower.SubDuration(key, subDelta)
		if err != nil {
			t.Fatalf("Failed to subtract duration: %v", err)
		}

		expected = stored - subDelta
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Test MulDuration
		factor := int64(3)
		stored, _ = tower.GetDuration(key) // Get current value before multiplication
		result, err = tower.MulDuration(key, factor)
		if err != nil {
			t.Fatalf("Failed to multiply duration: %v", err)
		}

		expected = stored * time.Duration(factor)
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Test DivDuration
		divisor := int64(2)
		stored, _ = tower.GetDuration(key) // Get current value before division
		result, err = tower.DivDuration(key, divisor)
		if err != nil {
			t.Fatalf("Failed to divide duration: %v", err)
		}

		expected = stored / time.Duration(divisor)
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test division by zero
	t.Run("DivisionByZero", func(t *testing.T) {
		key := "test:duration:divzero"
		err := tower.SetDuration(key, time.Hour)
		if err != nil {
			t.Fatalf("Failed to set duration: %v", err)
		}

		_, err = tower.DivDuration(key, 0)
		if err == nil {
			t.Error("Expected error when dividing by zero")
		}
	})

	// Test with different time units
	t.Run("DifferentTimeUnits", func(t *testing.T) {
		key := "test:duration:units"

		testCases := []struct {
			name     string
			duration time.Duration
		}{
			{"Nanoseconds", 500 * time.Nanosecond},
			{"Microseconds", 250 * time.Microsecond},
			{"Milliseconds", 750 * time.Millisecond},
			{"Seconds", 42 * time.Second},
			{"Minutes", 17 * time.Minute},
			{"Hours", 6 * time.Hour},
			{"Days", 24 * time.Hour},
			{"Weeks", 7 * 24 * time.Hour},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tower.SetDuration(key, tc.duration)
				if err != nil {
					t.Fatalf("Failed to set %s duration: %v", tc.name, err)
				}

				retrieved, err := tower.GetDuration(key)
				if err != nil {
					t.Fatalf("Failed to get %s duration: %v", tc.name, err)
				}

				if retrieved != tc.duration {
					t.Errorf("Expected %v, got %v", tc.duration, retrieved)
				}
			})
		}
	})

	// Test negative durations
	t.Run("NegativeDurations", func(t *testing.T) {
		key := "test:duration:negative"
		positiveDuration := 2 * time.Hour
		negativeDuration := -1 * time.Hour

		err := tower.SetDuration(key, positiveDuration)
		if err != nil {
			t.Fatalf("Failed to set positive duration: %v", err)
		}

		// Add negative duration (should subtract)
		result, err := tower.AddDuration(key, negativeDuration)
		if err != nil {
			t.Fatalf("Failed to add negative duration: %v", err)
		}

		expected := positiveDuration + negativeDuration // 1 hour
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Test with negative duration as base
		err = tower.SetDuration(key, negativeDuration)
		if err != nil {
			t.Fatalf("Failed to set negative duration: %v", err)
		}

		retrieved, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get negative duration: %v", err)
		}

		if retrieved != negativeDuration {
			t.Errorf("Expected %v, got %v", negativeDuration, retrieved)
		}

		// Test AbsDuration with negative value
		result, err = tower.AbsDuration(key)
		if err != nil {
			t.Fatalf("Failed to get absolute duration: %v", err)
		}

		expected = -negativeDuration // Should be positive
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test complex duration calculations
	t.Run("ComplexDurationCalculations", func(t *testing.T) {
		key := "test:duration:complex"

		// Start with 1 day
		initialDuration := 24 * time.Hour
		err := tower.SetDuration(key, initialDuration)
		if err != nil {
			t.Fatalf("Failed to set initial duration: %v", err)
		}

		// Add 6 hours: 24h + 6h = 30h
		result, err := tower.AddDuration(key, 6*time.Hour)
		if err != nil {
			t.Fatalf("Failed to add 6 hours: %v", err)
		}
		if result != 30*time.Hour {
			t.Errorf("Expected 30h, got %v", result)
		}

		// Subtract 90 minutes: 30h - 1.5h = 28.5h
		result, err = tower.SubDuration(key, 90*time.Minute)
		if err != nil {
			t.Fatalf("Failed to subtract 90 minutes: %v", err)
		}
		expected := 28*time.Hour + 30*time.Minute
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Multiply by 2: 28.5h * 2 = 57h
		result, err = tower.MulDuration(key, 2)
		if err != nil {
			t.Fatalf("Failed to multiply by 2: %v", err)
		}
		expected = 57 * time.Hour
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Divide by 3: 57h / 3 = 19h
		result, err = tower.DivDuration(key, 3)
		if err != nil {
			t.Fatalf("Failed to divide by 3: %v", err)
		}
		expected = 19 * time.Hour
		if result != expected {
			t.Errorf("Expected %v, got %v", expected, result)
		}
	})

	// Test SwapDuration
	t.Run("SwapDuration", func(t *testing.T) {
		key := "test:duration:swap"
		originalDuration := 45 * time.Minute
		newDuration := 2 * time.Hour

		err := tower.SetDuration(key, originalDuration)
		if err != nil {
			t.Fatalf("Failed to set original duration: %v", err)
		}

		oldDuration, err := tower.SwapDuration(key, newDuration)
		if err != nil {
			t.Fatalf("Failed to swap duration: %v", err)
		}

		if oldDuration != originalDuration {
			t.Errorf("Expected old duration %v, got %v", originalDuration, oldDuration)
		}

		// Verify new duration is stored
		stored, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get swapped duration: %v", err)
		}
		if stored != newDuration {
			t.Errorf("Expected stored duration %v, got %v", newDuration, stored)
		}
	})

	// Test zero duration
	t.Run("ZeroDuration", func(t *testing.T) {
		key := "test:duration:zero"
		zeroDuration := time.Duration(0)

		err := tower.SetDuration(key, zeroDuration)
		if err != nil {
			t.Fatalf("Failed to set zero duration: %v", err)
		}

		retrieved, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get zero duration: %v", err)
		}

		if retrieved != zeroDuration {
			t.Errorf("Expected %v, got %v", zeroDuration, retrieved)
		}

		// Test operations with zero duration
		_, err = tower.AddDuration(key, time.Hour)
		if err != nil {
			t.Fatalf("Failed to add to zero duration: %v", err)
		}

		_, err = tower.MulDuration(key, 5)
		if err != nil {
			t.Fatalf("Failed to multiply zero duration: %v", err)
		}

		// Division of zero should work
		_, err = tower.DivDuration(key, 2)
		if err != nil {
			t.Fatalf("Failed to divide zero duration: %v", err)
		}
	})

	// Test duration comparison scenarios
	t.Run("DurationComparisons", func(t *testing.T) {
		key := "test:duration:compare"

		// Test comparing different equivalent durations
		duration1 := 60 * time.Minute
		duration2 := 1 * time.Hour

		err := tower.SetDuration(key, duration1)
		if err != nil {
			t.Fatalf("Failed to set duration: %v", err)
		}

		// Even though they represent the same time, they should be equal
		if duration1 != duration2 {
			t.Errorf("Expected %v to equal %v", duration1, duration2)
		}

		// Test with microsecond precision
		preciseDuration := 1*time.Second + 500*time.Millisecond + 250*time.Microsecond
		err = tower.SetDuration(key, preciseDuration)
		if err != nil {
			t.Fatalf("Failed to set precise duration: %v", err)
		}

		retrieved, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get precise duration: %v", err)
		}

		if retrieved != preciseDuration {
			t.Errorf("Expected %v, got %v", preciseDuration, retrieved)
		}
	})

	// Test duration string representation and parsing
	t.Run("DurationStringOperations", func(t *testing.T) {
		key := "test:duration:string"

		// Test various duration formats
		testCases := []string{
			"1h30m45s",
			"2h15m",
			"45s",
			"500ms",
			"1.5h",
			"90m",
		}

		for _, durationStr := range testCases {
			t.Run(durationStr, func(t *testing.T) {
				parsedDuration, parseErr := time.ParseDuration(durationStr)
				if parseErr != nil {
					t.Fatalf("Failed to parse duration %s: %v", durationStr, parseErr)
				}

				err := tower.SetDuration(key, parsedDuration)
				if err != nil {
					t.Fatalf("Failed to set parsed duration: %v", err)
				}

				retrieved, err := tower.GetDuration(key)
				if err != nil {
					t.Fatalf("Failed to get parsed duration: %v", err)
				}

				if retrieved != parsedDuration {
					t.Errorf("Expected %v, got %v", parsedDuration, retrieved)
				}

				// Test string representation by converting back to string
				stringResult := retrieved.String()

				// Parse the string result back and compare
				reparsedDuration, parseErr := time.ParseDuration(stringResult)
				if parseErr != nil {
					t.Fatalf("Failed to parse duration string result: %v", parseErr)
				}

				if reparsedDuration != parsedDuration {
					t.Errorf("Round-trip failed: expected %v, got %v", parsedDuration, reparsedDuration)
				}
			})
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:duration:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetDuration(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.AddDuration(nonExistentKey, time.Hour)
		if err == nil {
			t.Error("Expected error when adding to non-existent key")
		}

		_, err = tower.MulDuration(nonExistentKey, 2)
		if err == nil {
			t.Error("Expected error when multiplying non-existent key")
		}

		_, err = tower.SwapDuration(nonExistentKey, time.Minute)
		if err == nil {
			t.Error("Expected error when swapping non-existent key")
		}
	})

	// Test extreme duration values
	t.Run("ExtremeDurationValues", func(t *testing.T) {
		key := "test:duration:extreme"

		// Test maximum duration
		maxDuration := time.Duration(1<<63 - 1) // Maximum int64 value
		err := tower.SetDuration(key, maxDuration)
		if err != nil {
			t.Fatalf("Failed to set max duration: %v", err)
		}

		retrieved, err := tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get max duration: %v", err)
		}

		if retrieved != maxDuration {
			t.Errorf("Expected %v, got %v", maxDuration, retrieved)
		}

		// Test minimum duration (most negative)
		minDuration := time.Duration(-1 << 63) // Minimum int64 value
		err = tower.SetDuration(key, minDuration)
		if err != nil {
			t.Fatalf("Failed to set min duration: %v", err)
		}

		retrieved, err = tower.GetDuration(key)
		if err != nil {
			t.Fatalf("Failed to get min duration: %v", err)
		}

		if retrieved != minDuration {
			t.Errorf("Expected %v, got %v", minDuration, retrieved)
		}
	})
}

