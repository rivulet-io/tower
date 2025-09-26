package op

import (
	"math/big"
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestBigIntOperations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "test-bigint.db",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test SetBigInt and GetBigInt
	t.Run("set and get BigInt", func(t *testing.T) {
		key := "test_bigint"
		value := big.NewInt(1234567890123456789)

		err := tower.SetBigInt(key, value)
		if err != nil {
			t.Errorf("SetBigInt failed: %v", err)
		}

		result, err := tower.GetBigInt(key)
		if err != nil {
			t.Errorf("GetBigInt failed: %v", err)
		}

		if result.Cmp(value) != 0 {
			t.Errorf("Expected %s, got %s", value.String(), result.String())
		}
	})

	// Test AddBigInt
	t.Run("add BigInt", func(t *testing.T) {
		key := "add_bigint"
		initial := big.NewInt(1000000000000000000)
		delta := big.NewInt(500000000000000000)

		tower.SetBigInt(key, initial)
		result, err := tower.AddBigInt(key, delta)
		if err != nil {
			t.Errorf("AddBigInt failed: %v", err)
		}

		expected := new(big.Int).Add(initial, delta)
		if result.Cmp(expected) != 0 {
			t.Errorf("Expected %s, got %s", expected.String(), result.String())
		}

		// Verify stored value
		stored, _ := tower.GetBigInt(key)
		if stored.Cmp(expected) != 0 {
			t.Errorf("Stored value mismatch: expected %s, got %s", expected.String(), stored.String())
		}
	})

	// Test MulBigInt
	t.Run("multiply BigInt", func(t *testing.T) {
		key := "mul_bigint"
		initial := big.NewInt(123456789)
		factor := big.NewInt(987654321)

		tower.SetBigInt(key, initial)
		result, err := tower.MulBigInt(key, factor)
		if err != nil {
			t.Errorf("MulBigInt failed: %v", err)
		}

		expected := new(big.Int).Mul(initial, factor)
		if result.Cmp(expected) != 0 {
			t.Errorf("Expected %s, got %s", expected.String(), result.String())
		}
	})

	// Test negative numbers
	t.Run("negative BigInt", func(t *testing.T) {
		key := "neg_bigint"
		value := big.NewInt(-1234567890123456789)

		err := tower.SetBigInt(key, value)
		if err != nil {
			t.Errorf("SetBigInt with negative failed: %v", err)
		}

		result, err := tower.GetBigInt(key)
		if err != nil {
			t.Errorf("GetBigInt for negative failed: %v", err)
		}

		if result.Cmp(value) != 0 {
			t.Errorf("Expected %s, got %s", value.String(), result.String())
		}

		// Test NegBigInt
		negResult, err := tower.NegBigInt(key)
		if err != nil {
			t.Errorf("NegBigInt failed: %v", err)
		}

		expected := new(big.Int).Neg(value)
		if negResult.Cmp(expected) != 0 {
			t.Errorf("NegBigInt: expected %s, got %s", expected.String(), negResult.String())
		}
	})
}

