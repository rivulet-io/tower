package tower

import (
	"math"
	"testing"
)

func TestDecimalOperations(t *testing.T) {
	tower, err := NewTower(&Options{
		Path:         "test-decimal.db",
		FS:           InMemory(),
		CacheSize:    NewSizeFromMegabytes(64),
		MemTableSize: NewSizeFromMegabytes(16),
		BytesPerSync: NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test SetDecimal and GetDecimal
	t.Run("set and get decimal", func(t *testing.T) {
		key := "test_decimal"
		coefficient := int64(12345)
		scale := int32(2) // Represents 123.45

		err := tower.SetDecimal(key, coefficient, scale)
		if err != nil {
			t.Errorf("SetDecimal failed: %v", err)
		}

		resultCoeff, resultScale, err := tower.GetDecimal(key)
		if err != nil {
			t.Errorf("GetDecimal failed: %v", err)
		}

		if resultCoeff != coefficient || resultScale != scale {
			t.Errorf("Expected (%d, %d), got (%d, %d)", coefficient, scale, resultCoeff, resultScale)
		}
	})

	// Test SetDecimalFromFloat and GetDecimalAsFloat
	t.Run("decimal from/to float", func(t *testing.T) {
		key := "float_decimal"
		value := 123.456789
		scale := int32(6)

		err := tower.SetDecimalFromFloat(key, value, scale)
		if err != nil {
			t.Errorf("SetDecimalFromFloat failed: %v", err)
		}

		result, err := tower.GetDecimalAsFloat(key)
		if err != nil {
			t.Errorf("GetDecimalAsFloat failed: %v", err)
		}

		// Allow small floating point differences
		if math.Abs(result-value) > 1e-10 {
			t.Errorf("Expected %f, got %f", value, result)
		}
	})

	// Test AddDecimal
	t.Run("add decimal", func(t *testing.T) {
		key := "add_decimal"
		// 100.50 (coefficient=10050, scale=2)
		tower.SetDecimal(key, 10050, 2)

		// Add 25.25 (coefficient=2525, scale=2)
		resultCoeff, resultScale, err := tower.AddDecimal(key, 2525, 2)
		if err != nil {
			t.Errorf("AddDecimal failed: %v", err)
		}

		// Expected: 125.75 (coefficient=12575, scale=2)
		expectedCoeff := int64(12575)
		expectedScale := int32(2)

		if resultCoeff != expectedCoeff || resultScale != expectedScale {
			t.Errorf("Expected (%d, %d), got (%d, %d)", expectedCoeff, expectedScale, resultCoeff, resultScale)
		}
	})

	// Test different scales
	t.Run("add decimal with different scales", func(t *testing.T) {
		key := "scale_decimal"
		// 10.5 (coefficient=105, scale=1)
		tower.SetDecimal(key, 105, 1)

		// Add 2.25 (coefficient=225, scale=2)
		resultCoeff, resultScale, err := tower.AddDecimal(key, 225, 2)
		if err != nil {
			t.Errorf("AddDecimal with different scales failed: %v", err)
		}

		// Expected: 12.75 (coefficient=1275, scale=2)
		expectedCoeff := int64(1275)
		expectedScale := int32(2)

		if resultCoeff != expectedCoeff || resultScale != expectedScale {
			t.Errorf("Expected (%d, %d), got (%d, %d)", expectedCoeff, expectedScale, resultCoeff, resultScale)
		}
	})

	// Test MulDecimal
	t.Run("multiply decimal", func(t *testing.T) {
		key := "mul_decimal"
		// 12.34 (coefficient=1234, scale=2)
		tower.SetDecimal(key, 1234, 2)

		// Multiply by 2.5 (coefficient=25, scale=1)
		resultCoeff, resultScale, err := tower.MulDecimal(key, 25, 1)
		if err != nil {
			t.Errorf("MulDecimal failed: %v", err)
		}

		// Expected: 30.85 (coefficient=3085, scale=2) -> 수정: 30.850 (coefficient=30850, scale=3)
		expectedCoeff := int64(30850)
		expectedScale := int32(3)

		if resultCoeff != expectedCoeff || resultScale != expectedScale {
			t.Errorf("Expected (%d, %d), got (%d, %d)", expectedCoeff, expectedScale, resultCoeff, resultScale)
		}
	})
}
