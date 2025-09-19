package op

import (
	"math"
	"math/big"
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestDecimalOperations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "test-decimal.db",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test SetDecimal and GetDecimal
	t.Run("set and get decimal", func(t *testing.T) {
		key := "test_decimal"
		coefficient := big.NewInt(12345)
		scale := int32(2) // Represents 123.45

		err := tower.SetDecimal(key, coefficient, scale)
		if err != nil {
			t.Errorf("SetDecimal failed: %v", err)
		}

		resultCoeff, resultScale, err := tower.GetDecimal(key)
		if err != nil {
			t.Errorf("GetDecimal failed: %v", err)
		}

		if resultCoeff.Cmp(coefficient) != 0 || resultScale != scale {
			t.Errorf("Expected (%s, %d), got (%s, %d)", coefficient.String(), scale, resultCoeff.String(), resultScale)
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
		tower.SetDecimal(key, big.NewInt(10050), 2)

		// Add 25.25 (coefficient=2525, scale=2)
		resultCoeff, resultScale, err := tower.AddDecimal(key, big.NewInt(2525), 2)
		if err != nil {
			t.Errorf("AddDecimal failed: %v", err)
		}

		// Expected: 125.75 (coefficient=12575, scale=2)
		expectedCoeff := big.NewInt(12575)
		expectedScale := int32(2)

		if resultCoeff.Cmp(expectedCoeff) != 0 || resultScale != expectedScale {
			t.Errorf("Expected (%s, %d), got (%s, %d)", expectedCoeff.String(), expectedScale, resultCoeff.String(), resultScale)
		}
	})

	// Test different scales
	t.Run("add decimal with different scales", func(t *testing.T) {
		key := "scale_decimal"
		// 10.5 (coefficient=105, scale=1)
		tower.SetDecimal(key, big.NewInt(105), 1)

		// Add 2.25 (coefficient=225, scale=2)
		resultCoeff, resultScale, err := tower.AddDecimal(key, big.NewInt(225), 2)
		if err != nil {
			t.Errorf("AddDecimal with different scales failed: %v", err)
		}

		// Expected: 12.75 (coefficient=1275, scale=2)
		expectedCoeff := big.NewInt(1275)
		expectedScale := int32(2)

		if resultCoeff.Cmp(expectedCoeff) != 0 || resultScale != expectedScale {
			t.Errorf("Expected (%s, %d), got (%s, %d)", expectedCoeff.String(), expectedScale, resultCoeff.String(), resultScale)
		}
	})

	// Test MulDecimal
	t.Run("multiply decimal", func(t *testing.T) {
		key := "mul_decimal"
		// 12.34 (coefficient=1234, scale=2)
		tower.SetDecimal(key, big.NewInt(1234), 2)

		// Multiply by 2.5 (coefficient=25, scale=1)
		resultCoeff, resultScale, err := tower.MulDecimal(key, big.NewInt(25), 1)
		if err != nil {
			t.Errorf("MulDecimal failed: %v", err)
		}

		// Expected: 30.85 (coefficient=3085, scale=2) -> 수정: 30.850 (coefficient=30850, scale=3)
		expectedCoeff := big.NewInt(30850)
		expectedScale := int32(3)

		if resultCoeff.Cmp(expectedCoeff) != 0 || resultScale != expectedScale {
			t.Errorf("Expected (%s, %d), got (%s, %d)", expectedCoeff.String(), expectedScale, resultCoeff.String(), resultScale)
		}
	})
}
