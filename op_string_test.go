package tower

import (
	"testing"
)

func TestStringOperations(t *testing.T) {
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

	// Test SetString and GetString
	t.Run("set and get string", func(t *testing.T) {
		key := "test_string"
		value := "hello world"

		err := tower.SetString(key, value)
		if err != nil {
			t.Errorf("SetString failed: %v", err)
		}

		result, err := tower.GetString(key)
		if err != nil {
			t.Errorf("GetString failed: %v", err)
		}

		if result != value {
			t.Errorf("Expected %s, got %s", value, result)
		}
	})

	// Test AppendString
	t.Run("append string", func(t *testing.T) {
		key := "append_test"
		initial := "hello"
		suffix := " world"

		tower.SetString(key, initial)
		result, err := tower.AppendString(key, suffix)
		if err != nil {
			t.Errorf("AppendString failed: %v", err)
		}

		expected := initial + suffix
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}

		// Verify the value is stored
		stored, _ := tower.GetString(key)
		if stored != expected {
			t.Errorf("Expected stored value %s, got %s", expected, stored)
		}
	})

	// Test PrependString
	t.Run("prepend string", func(t *testing.T) {
		key := "prepend_test"
		initial := "world"
		prefix := "hello "

		tower.SetString(key, initial)
		result, err := tower.PrependString(key, prefix)
		if err != nil {
			t.Errorf("PrependString failed: %v", err)
		}

		expected := prefix + initial
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	// Test ReplaceString
	t.Run("replace string", func(t *testing.T) {
		key := "replace_test"
		initial := "hello world hello"
		old := "hello"
		new := "hi"

		tower.SetString(key, initial)
		result, err := tower.ReplaceString(key, old, new)
		if err != nil {
			t.Errorf("ReplaceString failed: %v", err)
		}

		expected := "hi world hi"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	// Test ContainsString
	t.Run("contains string", func(t *testing.T) {
		key := "contains_test"
		value := "hello world"
		substr := "world"

		tower.SetString(key, value)
		result, err := tower.ContainsString(key, substr)
		if err != nil {
			t.Errorf("ContainsString failed: %v", err)
		}

		if !result {
			t.Error("Expected true, got false")
		}

		// Test non-existent substring
		result, err = tower.ContainsString(key, "xyz")
		if err != nil {
			t.Errorf("ContainsString failed: %v", err)
		}

		if result {
			t.Error("Expected false, got true")
		}
	})

	// Test StartsWithString
	t.Run("starts with string", func(t *testing.T) {
		key := "starts_test"
		value := "hello world"
		prefix := "hello"

		tower.SetString(key, value)
		result, err := tower.StartsWithString(key, prefix)
		if err != nil {
			t.Errorf("StartsWithString failed: %v", err)
		}

		if !result {
			t.Error("Expected true, got false")
		}
	})

	// Test EndsWithString
	t.Run("ends with string", func(t *testing.T) {
		key := "ends_test"
		value := "hello world"
		suffix := "world"

		tower.SetString(key, value)
		result, err := tower.EndsWithString(key, suffix)
		if err != nil {
			t.Errorf("EndsWithString failed: %v", err)
		}

		if !result {
			t.Error("Expected true, got false")
		}
	})

	// Test LengthString
	t.Run("length string", func(t *testing.T) {
		key := "length_test"
		value := "hello"

		tower.SetString(key, value)
		result, err := tower.LengthString(key)
		if err != nil {
			t.Errorf("LengthString failed: %v", err)
		}

		expected := len(value)
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	})

	// Test SubstringString
	t.Run("substring string", func(t *testing.T) {
		key := "substring_test"
		value := "hello world"
		start := 6
		length := 5

		tower.SetString(key, value)
		result, err := tower.SubstringString(key, start, length)
		if err != nil {
			t.Errorf("SubstringString failed: %v", err)
		}

		expected := "world"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	// Test CompareString
	t.Run("compare string", func(t *testing.T) {
		key := "compare_test"
		value := "apple"
		other := "banana"

		tower.SetString(key, value)
		result, err := tower.CompareString(key, other)
		if err != nil {
			t.Errorf("CompareString failed: %v", err)
		}

		if result >= 0 {
			t.Errorf("Expected negative value, got %d", result)
		}

		// Test equal strings
		result, err = tower.CompareString(key, "apple")
		if err != nil {
			t.Errorf("CompareString failed: %v", err)
		}

		if result != 0 {
			t.Errorf("Expected 0, got %d", result)
		}
	})

	// Test EqualString
	t.Run("equal string", func(t *testing.T) {
		key := "equal_test"
		value := "test"

		tower.SetString(key, value)
		result, err := tower.EqualString(key, "test")
		if err != nil {
			t.Errorf("EqualString failed: %v", err)
		}

		if !result {
			t.Error("Expected true, got false")
		}

		result, err = tower.EqualString(key, "other")
		if err != nil {
			t.Errorf("EqualString failed: %v", err)
		}

		if result {
			t.Error("Expected false, got true")
		}
	})

	// Test UpperString
	t.Run("upper string", func(t *testing.T) {
		key := "upper_test"
		value := "hello world"

		tower.SetString(key, value)
		result, err := tower.UpperString(key)
		if err != nil {
			t.Errorf("UpperString failed: %v", err)
		}

		expected := "HELLO WORLD"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}

		// Verify the original value is updated
		stored, _ := tower.GetString(key)
		if stored != expected {
			t.Errorf("Expected stored value %s, got %s", expected, stored)
		}
	})

	// Test LowerString
	t.Run("lower string", func(t *testing.T) {
		key := "lower_test"
		value := "HELLO WORLD"

		tower.SetString(key, value)
		result, err := tower.LowerString(key)
		if err != nil {
			t.Errorf("LowerString failed: %v", err)
		}

		expected := "hello world"
		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}

		// Verify the original value is updated
		stored, _ := tower.GetString(key)
		if stored != expected {
			t.Errorf("Expected stored value %s, got %s", expected, stored)
		}
	})
}
