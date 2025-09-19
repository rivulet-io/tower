package op

import (
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestBoolOperations(t *testing.T) {
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

	// Test SetBool and GetBool
	t.Run("set and get bool", func(t *testing.T) {
		key := "test_bool"
		value := true

		err := tower.SetBool(key, value)
		if err != nil {
			t.Errorf("SetBool failed: %v", err)
		}

		result, err := tower.GetBool(key)
		if err != nil {
			t.Errorf("GetBool failed: %v", err)
		}

		if result != value {
			t.Errorf("Expected %t, got %t", value, result)
		}

		// Test with false
		err = tower.SetBool(key, false)
		if err != nil {
			t.Errorf("SetBool failed: %v", err)
		}

		result, err = tower.GetBool(key)
		if err != nil {
			t.Errorf("GetBool failed: %v", err)
		}

		if result != false {
			t.Errorf("Expected false, got %t", result)
		}
	})

	// Test AndBool
	t.Run("and bool", func(t *testing.T) {
		key := "and_test"

		// Test true AND true = true
		tower.SetBool(key, true)
		result, err := tower.AndBool(key, true)
		if err != nil {
			t.Errorf("AndBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test true AND false = false
		tower.SetBool(key, true)
		result, err = tower.AndBool(key, false)
		if err != nil {
			t.Errorf("AndBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Test false AND true = false
		tower.SetBool(key, false)
		result, err = tower.AndBool(key, true)
		if err != nil {
			t.Errorf("AndBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Test false AND false = false
		tower.SetBool(key, false)
		result, err = tower.AndBool(key, false)
		if err != nil {
			t.Errorf("AndBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Verify the value is stored
		stored, _ := tower.GetBool(key)
		if stored != false {
			t.Errorf("Expected stored value false, got %t", stored)
		}
	})

	// Test OrBool
	t.Run("or bool", func(t *testing.T) {
		key := "or_test"

		// Test true OR true = true
		tower.SetBool(key, true)
		result, err := tower.OrBool(key, true)
		if err != nil {
			t.Errorf("OrBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test true OR false = true
		tower.SetBool(key, true)
		result, err = tower.OrBool(key, false)
		if err != nil {
			t.Errorf("OrBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test false OR true = true
		tower.SetBool(key, false)
		result, err = tower.OrBool(key, true)
		if err != nil {
			t.Errorf("OrBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test false OR false = false
		tower.SetBool(key, false)
		result, err = tower.OrBool(key, false)
		if err != nil {
			t.Errorf("OrBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}
	})

	// Test XorBool
	t.Run("xor bool", func(t *testing.T) {
		key := "xor_test"

		// Test true XOR true = false
		tower.SetBool(key, true)
		result, err := tower.XorBool(key, true)
		if err != nil {
			t.Errorf("XorBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Test true XOR false = true
		tower.SetBool(key, true)
		result, err = tower.XorBool(key, false)
		if err != nil {
			t.Errorf("XorBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test false XOR true = true
		tower.SetBool(key, false)
		result, err = tower.XorBool(key, true)
		if err != nil {
			t.Errorf("XorBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Test false XOR false = false
		tower.SetBool(key, false)
		result, err = tower.XorBool(key, false)
		if err != nil {
			t.Errorf("XorBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}
	})

	// Test NotBool
	t.Run("not bool", func(t *testing.T) {
		key := "not_test"

		// Test NOT true = false
		tower.SetBool(key, true)
		result, err := tower.NotBool(key)
		if err != nil {
			t.Errorf("NotBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Verify the value is stored
		stored, _ := tower.GetBool(key)
		if stored != false {
			t.Errorf("Expected stored value false, got %t", stored)
		}

		// Test NOT false = true
		tower.SetBool(key, false)
		result, err = tower.NotBool(key)
		if err != nil {
			t.Errorf("NotBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}
	})

	// Test EqualBool
	t.Run("equal bool", func(t *testing.T) {
		key := "equal_test"

		// Test true == true
		tower.SetBool(key, true)
		result, err := tower.EqualBool(key, true)
		if err != nil {
			t.Errorf("EqualBool failed: %v", err)
		}
		if !result {
			t.Error("Expected true, got false")
		}

		// Test true == false
		result, err = tower.EqualBool(key, false)
		if err != nil {
			t.Errorf("EqualBool failed: %v", err)
		}
		if result {
			t.Error("Expected false, got true")
		}

		// Test false == false
		tower.SetBool(key, false)
		result, err = tower.EqualBool(key, false)
		if err != nil {
			t.Errorf("EqualBool failed: %v", err)
		}
		if !result {
			t.Error("Expected true, got false")
		}
	})

	// Test ToggleBool
	t.Run("toggle bool", func(t *testing.T) {
		key := "toggle_test"

		// Test toggle true to false
		tower.SetBool(key, true)
		result, err := tower.ToggleBool(key)
		if err != nil {
			t.Errorf("ToggleBool failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Test toggle false to true
		result, err = tower.ToggleBool(key)
		if err != nil {
			t.Errorf("ToggleBool failed: %v", err)
		}
		if result != true {
			t.Errorf("Expected true, got %t", result)
		}

		// Verify the final value is stored
		stored, _ := tower.GetBool(key)
		if stored != true {
			t.Errorf("Expected stored value true, got %t", stored)
		}
	})

	// Test SetBoolIfEqual
	t.Run("set bool if equal", func(t *testing.T) {
		key := "conditional_test"

		// Test setting when condition is met
		tower.SetBool(key, true)
		result, err := tower.SetBoolIfEqual(key, true, false)
		if err != nil {
			t.Errorf("SetBoolIfEqual failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false, got %t", result)
		}

		// Verify the value was changed
		stored, _ := tower.GetBool(key)
		if stored != false {
			t.Errorf("Expected stored value false, got %t", stored)
		}

		// Test when condition is not met
		result, err = tower.SetBoolIfEqual(key, true, true)
		if err != nil {
			t.Errorf("SetBoolIfEqual failed: %v", err)
		}
		if result != false {
			t.Errorf("Expected false (unchanged), got %t", result)
		}
	})
}
