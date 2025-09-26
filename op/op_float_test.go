package op

import (
	"math"
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestFloatOperations(t *testing.T) {
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

	// Test SetFloat and GetFloat
	t.Run("SetFloat_GetFloat", func(t *testing.T) {
		key := "test:float"
		testValue := 3.14159

		err := tower.SetFloat(key, testValue)
		if err != nil {
			t.Fatalf("Failed to set float: %v", err)
		}

		retrieved, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get float: %v", err)
		}

		if retrieved != testValue {
			t.Errorf("Expected %f, got %f", testValue, retrieved)
		}
	})

	// Test arithmetic operations
	t.Run("ArithmeticOperations", func(t *testing.T) {
		key := "test:float:arithmetic"
		initialValue := 10.5

		err := tower.SetFloat(key, initialValue)
		if err != nil {
			t.Fatalf("Failed to set initial float: %v", err)
		}

		// Test AddFloat
		delta := 5.25
		result, err := tower.AddFloat(key, delta)
		if err != nil {
			t.Fatalf("Failed to add float: %v", err)
		}

		expected := initialValue + delta
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}

		// Verify stored value
		stored, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get stored float: %v", err)
		}
		if stored != expected {
			t.Errorf("Stored value expected %f, got %f", expected, stored)
		}

		// Test SubFloat
		subDelta := 3.75
		result, err = tower.SubFloat(key, subDelta)
		if err != nil {
			t.Fatalf("Failed to subtract float: %v", err)
		}

		expected = stored - subDelta
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}

		// Test MulFloat
		factor := 2.0
		stored, _ = tower.GetFloat(key) // Get current value before multiplication
		result, err = tower.MulFloat(key, factor)
		if err != nil {
			t.Fatalf("Failed to multiply float: %v", err)
		}

		expected = stored * factor
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}

		// Test DivFloat
		divisor := 4.0
		stored, _ = tower.GetFloat(key) // Get current value before division
		result, err = tower.DivFloat(key, divisor)
		if err != nil {
			t.Fatalf("Failed to divide float: %v", err)
		}

		expected = stored / divisor
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}
	})

	// Test division by zero
	t.Run("DivisionByZero", func(t *testing.T) {
		key := "test:float:divzero"
		err := tower.SetFloat(key, 10.0)
		if err != nil {
			t.Fatalf("Failed to set float: %v", err)
		}

		_, err = tower.DivFloat(key, 0.0)
		if err == nil {
			t.Error("Expected error when dividing by zero")
		}
	})

	// Test unary operations
	t.Run("UnaryOperations", func(t *testing.T) {
		key := "test:float:unary"

		// Test NegFloat
		positiveValue := 7.5
		err := tower.SetFloat(key, positiveValue)
		if err != nil {
			t.Fatalf("Failed to set positive float: %v", err)
		}

		result, err := tower.NegFloat(key)
		if err != nil {
			t.Fatalf("Failed to negate float: %v", err)
		}

		expected := -positiveValue
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}

		// Test NegFloat again (should become positive)
		result, err = tower.NegFloat(key)
		if err != nil {
			t.Fatalf("Failed to negate float again: %v", err)
		}

		if result != positiveValue {
			t.Errorf("Expected %f, got %f", positiveValue, result)
		}

		// Test AbsFloat with negative value
		negativeValue := -15.25
		err = tower.SetFloat(key, negativeValue)
		if err != nil {
			t.Fatalf("Failed to set negative float: %v", err)
		}

		result, err = tower.AbsFloat(key)
		if err != nil {
			t.Fatalf("Failed to get absolute value: %v", err)
		}

		expected = math.Abs(negativeValue)
		if result != expected {
			t.Errorf("Expected %f, got %f", expected, result)
		}

		// Test AbsFloat with positive value
		err = tower.SetFloat(key, positiveValue)
		if err != nil {
			t.Fatalf("Failed to set positive float: %v", err)
		}

		result, err = tower.AbsFloat(key)
		if err != nil {
			t.Fatalf("Failed to get absolute value of positive: %v", err)
		}

		if result != positiveValue {
			t.Errorf("Expected %f, got %f", positiveValue, result)
		}
	})

	// Test SwapFloat
	t.Run("SwapFloat", func(t *testing.T) {
		key := "test:float:swap"
		originalValue := 12.34
		newValue := 56.78

		err := tower.SetFloat(key, originalValue)
		if err != nil {
			t.Fatalf("Failed to set original float: %v", err)
		}

		oldValue, err := tower.SwapFloat(key, newValue)
		if err != nil {
			t.Fatalf("Failed to swap float: %v", err)
		}

		if oldValue != originalValue {
			t.Errorf("Expected old value %f, got %f", originalValue, oldValue)
		}

		// Verify new value is stored
		stored, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get swapped float: %v", err)
		}
		if stored != newValue {
			t.Errorf("Expected stored value %f, got %f", newValue, stored)
		}
	})

	// Test with special float values
	t.Run("SpecialFloatValues", func(t *testing.T) {
		key := "test:float:special"

		testCases := []struct {
			name  string
			value float64
		}{
			{"Zero", 0.0},
			{"NegativeZero", -0},
			{"Infinity", math.Inf(1)},
			{"NegativeInfinity", math.Inf(-1)},
			{"NaN", math.NaN()},
			{"MaxFloat64", math.MaxFloat64},
			{"SmallestNonZeroFloat64", math.SmallestNonzeroFloat64},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tower.SetFloat(key, tc.value)
				if err != nil {
					t.Fatalf("Failed to set %s float: %v", tc.name, err)
				}

				retrieved, err := tower.GetFloat(key)
				if err != nil {
					t.Fatalf("Failed to get %s float: %v", tc.name, err)
				}

				// Special handling for NaN comparison
				if math.IsNaN(tc.value) {
					if !math.IsNaN(retrieved) {
						t.Errorf("Expected NaN, got %f", retrieved)
					}
				} else {
					if retrieved != tc.value {
						t.Errorf("Expected %f, got %f", tc.value, retrieved)
					}
				}
			})
		}
	})

	// Test precision and rounding
	t.Run("PrecisionAndRounding", func(t *testing.T) {
		key := "test:float:precision"

		// Test with high precision values
		highPrecisionValue := 1.23456789012345678901234567890
		err := tower.SetFloat(key, highPrecisionValue)
		if err != nil {
			t.Fatalf("Failed to set high precision float: %v", err)
		}

		retrieved, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get high precision float: %v", err)
		}

		// float64 has limited precision, so we check if they're close enough
		const epsilon = 1e-15
		if math.Abs(retrieved-highPrecisionValue) > epsilon {
			t.Errorf("Expected %f, got %f (diff: %e)", highPrecisionValue, retrieved, math.Abs(retrieved-highPrecisionValue))
		}

		// Test arithmetic with potential precision issues
		_, err = tower.AddFloat(key, 0.1)
		if err != nil {
			t.Fatalf("Failed to add to high precision float: %v", err)
		}

		_, err = tower.MulFloat(key, 1.1)
		if err != nil {
			t.Fatalf("Failed to multiply high precision float: %v", err)
		}
	})

	// Test chained operations
	t.Run("ChainedOperations", func(t *testing.T) {
		key := "test:float:chained"
		initialValue := 100.0

		err := tower.SetFloat(key, initialValue)
		if err != nil {
			t.Fatalf("Failed to set initial float: %v", err)
		}

		// Chain multiple operations: 100 + 50 - 25 * 2 / 5 = 50
		result, err := tower.AddFloat(key, 50.0)
		if err != nil {
			t.Fatalf("Failed in chained operation (add): %v", err)
		}
		if result != 150.0 {
			t.Errorf("After add: expected 150.0, got %f", result)
		}

		result, err = tower.SubFloat(key, 25.0)
		if err != nil {
			t.Fatalf("Failed in chained operation (sub): %v", err)
		}
		if result != 125.0 {
			t.Errorf("After sub: expected 125.0, got %f", result)
		}

		result, err = tower.MulFloat(key, 2.0)
		if err != nil {
			t.Fatalf("Failed in chained operation (mul): %v", err)
		}
		if result != 250.0 {
			t.Errorf("After mul: expected 250.0, got %f", result)
		}

		result, err = tower.DivFloat(key, 5.0)
		if err != nil {
			t.Fatalf("Failed in chained operation (div): %v", err)
		}
		if result != 50.0 {
			t.Errorf("After div: expected 50.0, got %f", result)
		}

		// Verify final stored value
		final, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get final float: %v", err)
		}
		if final != 50.0 {
			t.Errorf("Final stored value: expected 50.0, got %f", final)
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:float:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetFloat(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.AddFloat(nonExistentKey, 1.0)
		if err == nil {
			t.Error("Expected error when adding to non-existent key")
		}

		_, err = tower.MulFloat(nonExistentKey, 2.0)
		if err == nil {
			t.Error("Expected error when multiplying non-existent key")
		}

		_, err = tower.SwapFloat(nonExistentKey, 3.0)
		if err == nil {
			t.Error("Expected error when swapping non-existent key")
		}
	})

	// Test with very large and very small numbers
	t.Run("ExtremeValues", func(t *testing.T) {
		key := "test:float:extreme"

		// Test with very large number
		largeNumber := 1e308
		err := tower.SetFloat(key, largeNumber)
		if err != nil {
			t.Fatalf("Failed to set large number: %v", err)
		}

		retrieved, err := tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get large number: %v", err)
		}
		if retrieved != largeNumber {
			t.Errorf("Expected %e, got %e", largeNumber, retrieved)
		}

		// Test with very small number
		smallNumber := 1e-307
		err = tower.SetFloat(key, smallNumber)
		if err != nil {
			t.Fatalf("Failed to set small number: %v", err)
		}

		retrieved, err = tower.GetFloat(key)
		if err != nil {
			t.Fatalf("Failed to get small number: %v", err)
		}
		if retrieved != smallNumber {
			t.Errorf("Expected %e, got %e", smallNumber, retrieved)
		}

		// Test arithmetic with extreme values
		_, err = tower.AddFloat(key, 1e-308)
		if err != nil {
			t.Fatalf("Failed to add to small number: %v", err)
		}

		_, err = tower.MulFloat(key, 1e100)
		if err != nil {
			t.Fatalf("Failed to multiply small number: %v", err)
		}
	})
}

