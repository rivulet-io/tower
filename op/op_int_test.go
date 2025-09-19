package op

import (
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestIntOperations(t *testing.T) {
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

	// Test SetInt and GetInt
	t.Run("set and get int", func(t *testing.T) {
		key := "test_int"
		value := int64(42)

		err := tower.SetInt(key, value)
		if err != nil {
			t.Errorf("SetInt failed: %v", err)
		}

		result, err := tower.GetInt(key)
		if err != nil {
			t.Errorf("GetInt failed: %v", err)
		}

		if result != value {
			t.Errorf("Expected %d, got %d", value, result)
		}
	})

	// Test AddInt
	t.Run("add int", func(t *testing.T) {
		key := "add_test"
		initial := int64(10)
		delta := int64(5)

		tower.SetInt(key, initial)
		result, err := tower.AddInt(key, delta)
		if err != nil {
			t.Errorf("AddInt failed: %v", err)
		}

		expected := initial + delta
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Verify the value is stored
		stored, _ := tower.GetInt(key)
		if stored != expected {
			t.Errorf("Expected stored value %d, got %d", expected, stored)
		}
	})

	// Test SubInt
	t.Run("sub int", func(t *testing.T) {
		key := "sub_test"
		initial := int64(20)
		delta := int64(5)

		tower.SetInt(key, initial)
		result, err := tower.SubInt(key, delta)
		if err != nil {
			t.Errorf("SubInt failed: %v", err)
		}

		expected := initial - delta
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test IncInt
	t.Run("inc int", func(t *testing.T) {
		key := "inc_test"
		initial := int64(10)

		tower.SetInt(key, initial)
		result, err := tower.IncInt(key)
		if err != nil {
			t.Errorf("IncInt failed: %v", err)
		}

		expected := initial + 1
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test DecInt
	t.Run("dec int", func(t *testing.T) {
		key := "dec_test"
		initial := int64(10)

		tower.SetInt(key, initial)
		result, err := tower.DecInt(key)
		if err != nil {
			t.Errorf("DecInt failed: %v", err)
		}

		expected := initial - 1
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test MulInt
	t.Run("mul int", func(t *testing.T) {
		key := "mul_test"
		initial := int64(5)
		factor := int64(3)

		tower.SetInt(key, initial)
		result, err := tower.MulInt(key, factor)
		if err != nil {
			t.Errorf("MulInt failed: %v", err)
		}

		expected := initial * factor
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test DivInt
	t.Run("div int", func(t *testing.T) {
		key := "div_test"
		initial := int64(15)
		divisor := int64(3)

		tower.SetInt(key, initial)
		result, err := tower.DivInt(key, divisor)
		if err != nil {
			t.Errorf("DivInt failed: %v", err)
		}

		expected := initial / divisor
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test division by zero
		_, err = tower.DivInt(key, 0)
		if err == nil {
			t.Error("Expected error for division by zero")
		}
	})

	// Test ModInt
	t.Run("mod int", func(t *testing.T) {
		key := "mod_test"
		initial := int64(17)
		modulus := int64(5)

		tower.SetInt(key, initial)
		result, err := tower.ModInt(key, modulus)
		if err != nil {
			t.Errorf("ModInt failed: %v", err)
		}

		expected := initial % modulus
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test modulo by zero
		_, err = tower.ModInt(key, 0)
		if err == nil {
			t.Error("Expected error for modulo by zero")
		}
	})

	// Test NegInt
	t.Run("neg int", func(t *testing.T) {
		key := "neg_test"
		initial := int64(42)

		tower.SetInt(key, initial)
		result, err := tower.NegInt(key)
		if err != nil {
			t.Errorf("NegInt failed: %v", err)
		}

		expected := -initial
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test AbsInt
	t.Run("abs int", func(t *testing.T) {
		key := "abs_test"
		initial := int64(-42)

		tower.SetInt(key, initial)
		result, err := tower.AbsInt(key)
		if err != nil {
			t.Errorf("AbsInt failed: %v", err)
		}

		expected := int64(42)
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test with positive number
		tower.SetInt(key, 42)
		result, err = tower.AbsInt(key)
		if err != nil {
			t.Errorf("AbsInt failed: %v", err)
		}

		if result != 42 {
			t.Errorf("Expected 42, got %d", result)
		}
	})

	// Test SwapInt
	t.Run("swap int", func(t *testing.T) {
		key := "swap_test"
		initial := int64(10)
		newValue := int64(20)

		tower.SetInt(key, initial)
		result, err := tower.SwapInt(key, newValue)
		if err != nil {
			t.Errorf("SwapInt failed: %v", err)
		}

		if result != initial {
			t.Errorf("Expected %d, got %d", initial, result)
		}

		// Verify new value is stored
		stored, _ := tower.GetInt(key)
		if stored != newValue {
			t.Errorf("Expected stored value %d, got %d", newValue, stored)
		}
	})

	// Test CompareInt
	t.Run("compare int", func(t *testing.T) {
		key := "compare_test"
		value := int64(10)

		tower.SetInt(key, value)

		// Test less than
		result, err := tower.CompareInt(key, 15)
		if err != nil {
			t.Errorf("CompareInt failed: %v", err)
		}
		if result >= 0 {
			t.Errorf("Expected negative value, got %d", result)
		}

		// Test equal
		result, err = tower.CompareInt(key, 10)
		if err != nil {
			t.Errorf("CompareInt failed: %v", err)
		}
		if result != 0 {
			t.Errorf("Expected 0, got %d", result)
		}

		// Test greater than
		result, err = tower.CompareInt(key, 5)
		if err != nil {
			t.Errorf("CompareInt failed: %v", err)
		}
		if result <= 0 {
			t.Errorf("Expected positive value, got %d", result)
		}
	})

	// Test SetIntIfGreater
	t.Run("set int if greater", func(t *testing.T) {
		key := "greater_test"
		initial := int64(10)
		greaterValue := int64(15)
		smallerValue := int64(5)

		tower.SetInt(key, initial)

		// Test with greater value
		result, err := tower.SetIntIfGreater(key, greaterValue)
		if err != nil {
			t.Errorf("SetIntIfGreater failed: %v", err)
		}
		if result != greaterValue {
			t.Errorf("Expected %d, got %d", greaterValue, result)
		}

		// Test with smaller value
		result, err = tower.SetIntIfGreater(key, smallerValue)
		if err != nil {
			t.Errorf("SetIntIfGreater failed: %v", err)
		}
		if result != greaterValue {
			t.Errorf("Expected %d, got %d", greaterValue, result)
		}
	})

	// Test SetIntIfLess
	t.Run("set int if less", func(t *testing.T) {
		key := "less_test"
		initial := int64(10)
		smallerValue := int64(5)
		greaterValue := int64(15)

		tower.SetInt(key, initial)

		// Test with smaller value
		result, err := tower.SetIntIfLess(key, smallerValue)
		if err != nil {
			t.Errorf("SetIntIfLess failed: %v", err)
		}
		if result != smallerValue {
			t.Errorf("Expected %d, got %d", smallerValue, result)
		}

		// Test with greater value
		result, err = tower.SetIntIfLess(key, greaterValue)
		if err != nil {
			t.Errorf("SetIntIfLess failed: %v", err)
		}
		if result != smallerValue {
			t.Errorf("Expected %d, got %d", smallerValue, result)
		}
	})

	// Test ClampInt
	t.Run("clamp int", func(t *testing.T) {
		key := "clamp_test"
		min := int64(5)
		max := int64(15)

		// Test value below min
		tower.SetInt(key, 2)
		result, err := tower.ClampInt(key, min, max)
		if err != nil {
			t.Errorf("ClampInt failed: %v", err)
		}
		if result != min {
			t.Errorf("Expected %d, got %d", min, result)
		}

		// Test value above max
		tower.SetInt(key, 20)
		result, err = tower.ClampInt(key, min, max)
		if err != nil {
			t.Errorf("ClampInt failed: %v", err)
		}
		if result != max {
			t.Errorf("Expected %d, got %d", max, result)
		}

		// Test value within range
		tower.SetInt(key, 10)
		result, err = tower.ClampInt(key, min, max)
		if err != nil {
			t.Errorf("ClampInt failed: %v", err)
		}
		if result != 10 {
			t.Errorf("Expected 10, got %d", result)
		}
	})

	// Test bitwise operations
	t.Run("bitwise operations", func(t *testing.T) {
		key := "bitwise_test"
		value := int64(12) // 1100 in binary
		mask := int64(10)  // 1010 in binary

		// Test AndInt
		tower.SetInt(key, value)
		result, err := tower.AndInt(key, mask)
		if err != nil {
			t.Errorf("AndInt failed: %v", err)
		}
		expected := value & mask // 1000 = 8
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test OrInt
		tower.SetInt(key, value)
		result, err = tower.OrInt(key, mask)
		if err != nil {
			t.Errorf("OrInt failed: %v", err)
		}
		expected = value | mask // 1110 = 14
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test XorInt
		tower.SetInt(key, value)
		result, err = tower.XorInt(key, mask)
		if err != nil {
			t.Errorf("XorInt failed: %v", err)
		}
		expected = value ^ mask // 0110 = 6
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test shift operations
	t.Run("shift operations", func(t *testing.T) {
		key := "shift_test"
		value := int64(8) // 1000 in binary

		// Test ShiftLeftInt
		tower.SetInt(key, value)
		result, err := tower.ShiftLeftInt(key, 2)
		if err != nil {
			t.Errorf("ShiftLeftInt failed: %v", err)
		}
		expected := value << 2 // 100000 = 32
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}

		// Test ShiftRightInt
		tower.SetInt(key, value)
		result, err = tower.ShiftRightInt(key, 1)
		if err != nil {
			t.Errorf("ShiftRightInt failed: %v", err)
		}
		expected = value >> 1 // 0100 = 4
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})
}
