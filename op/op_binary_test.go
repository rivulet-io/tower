package op

import (
	"bytes"
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestBinaryOperations(t *testing.T) {
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

	// Test SetBinary and GetBinary
	t.Run("SetBinary_GetBinary", func(t *testing.T) {
		key := "test:binary"
		testData := []byte("Hello, Binary World!")

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		retrieved, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get binary: %v", err)
		}

		if !bytes.Equal(retrieved, testData) {
			t.Errorf("Expected %v, got %v", testData, retrieved)
		}
	})

	// Test AppendBinary
	t.Run("AppendBinary", func(t *testing.T) {
		key := "test:binary:append"
		initialData := []byte("Hello")
		appendData := []byte(", World!")

		err := tower.SetBinary(key, initialData)
		if err != nil {
			t.Fatalf("Failed to set initial binary: %v", err)
		}

		result, err := tower.AppendBinary(key, appendData)
		if err != nil {
			t.Fatalf("Failed to append binary: %v", err)
		}

		expected := append(initialData, appendData...)
		if !bytes.Equal(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify stored value
		stored, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get stored binary: %v", err)
		}
		if !bytes.Equal(stored, expected) {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}
	})

	// Test PrependBinary
	t.Run("PrependBinary", func(t *testing.T) {
		key := "test:binary:prepend"
		initialData := []byte("World!")
		prependData := []byte("Hello, ")

		err := tower.SetBinary(key, initialData)
		if err != nil {
			t.Fatalf("Failed to set initial binary: %v", err)
		}

		result, err := tower.PrependBinary(key, prependData)
		if err != nil {
			t.Fatalf("Failed to prepend binary: %v", err)
		}

		expected := append(prependData, initialData...)
		if !bytes.Equal(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify stored value
		stored, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get stored binary: %v", err)
		}
		if !bytes.Equal(stored, expected) {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}
	})

	// Test LengthBinary
	t.Run("LengthBinary", func(t *testing.T) {
		key := "test:binary:length"
		testData := []byte("Test data for length calculation")

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		length, err := tower.GetBinaryLength(key)
		if err != nil {
			t.Fatalf("Failed to get binary length: %v", err)
		}

		expectedLength := len(testData)
		if length != expectedLength {
			t.Errorf("Expected length %d, got %d", expectedLength, length)
		}

		// Test with empty data
		err = tower.SetBinary(key, []byte{})
		if err != nil {
			t.Fatalf("Failed to set empty binary: %v", err)
		}

		length, err = tower.GetBinaryLength(key)
		if err != nil {
			t.Fatalf("Failed to get empty binary length: %v", err)
		}

		if length != 0 {
			t.Errorf("Expected length 0 for empty data, got %d", length)
		}
	})

	// Test with different types of binary data
	t.Run("DifferentBinaryData", func(t *testing.T) {
		key := "test:binary:types"

		testCases := []struct {
			name string
			data []byte
		}{
			{"Empty", []byte{}},
			{"SingleByte", []byte{0x42}},
			{"NullBytes", []byte{0x00, 0x00, 0x00}},
			{"AllBytes", func() []byte {
				data := make([]byte, 256)
				for i := 0; i < 256; i++ {
					data[i] = byte(i)
				}
				return data
			}()},
			{"UTF8Text", []byte("Hello, 世界! 🚀💻")},
			{"BinaryPattern", []byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := tower.SetBinary(key, tc.data)
				if err != nil {
					t.Fatalf("Failed to set %s binary: %v", tc.name, err)
				}

				retrieved, err := tower.GetBinary(key)
				if err != nil {
					t.Fatalf("Failed to get %s binary: %v", tc.name, err)
				}

				if !bytes.Equal(retrieved, tc.data) {
					t.Errorf("Expected %v, got %v", tc.data, retrieved)
				}

				// Test length
				length, err := tower.GetBinaryLength(key)
				if err != nil {
					t.Fatalf("Failed to get %s binary length: %v", tc.name, err)
				}

				if length != len(tc.data) {
					t.Errorf("Expected length %d, got %d", len(tc.data), length)
				}
			})
		}
	})

	// Test SubBinary
	t.Run("SubBinaryOperations", func(t *testing.T) {
		key := "test:binary:sub"
		testData := []byte("0123456789ABCDEF")

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		// Test SubBinary
		start, length := 5, 5
		sub, err := tower.GetBinarySubstring(key, start, length)
		if err != nil {
			t.Fatalf("Failed to get sub binary: %v", err)
		}

		expected := testData[start : start+length]
		if !bytes.Equal(sub, expected) {
			t.Errorf("Expected %v, got %v", expected, sub)
		}

		// Test edge cases
		// Get from beginning
		sub, err = tower.GetBinarySubstring(key, 0, 5)
		if err != nil {
			t.Fatalf("Failed to get sub binary from beginning: %v", err)
		}
		expected = testData[0:5]
		if !bytes.Equal(sub, expected) {
			t.Errorf("Expected %v, got %v", expected, sub)
		}

		// Get with length beyond end (should truncate)
		sub, err = tower.GetBinarySubstring(key, 10, 20)
		if err != nil {
			t.Fatalf("Failed to get sub binary beyond end: %v", err)
		}
		expected = testData[10:]
		if !bytes.Equal(sub, expected) {
			t.Errorf("Expected %v, got %v", expected, sub)
		}

		// Test invalid ranges
		_, err = tower.GetBinarySubstring(key, -1, 5) // negative start
		if err == nil {
			t.Error("Expected error for negative start")
		}

		_, err = tower.GetBinarySubstring(key, len(testData), 1) // start beyond length
		if err == nil {
			t.Error("Expected error for start beyond length")
		}
	})

	// Test CompareBinary
	t.Run("CompareBinary", func(t *testing.T) {
		key := "test:binary:compare"
		baseData := []byte("compare")

		err := tower.SetBinary(key, baseData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		// Test equal comparison
		equal, err := tower.CompareBinaryEqual(key, baseData)
		if err != nil {
			t.Fatalf("Failed to compare equal binary: %v", err)
		}
		if !equal {
			t.Error("Expected binary data to be equal")
		}

		// Test not equal comparison
		differentData := []byte("different")
		equal, err = tower.CompareBinaryEqual(key, differentData)
		if err != nil {
			t.Fatalf("Failed to compare different binary: %v", err)
		}
		if equal {
			t.Error("Expected binary data to be different")
		}

		// Test CompareBinary
		result, err := tower.CompareBinary(key, []byte("compare"))
		if err != nil {
			t.Fatalf("Failed to compare binary: %v", err)
		}
		if result != 0 {
			t.Errorf("Expected 0 (equal), got %d", result)
		}

		result, err = tower.CompareBinary(key, []byte("alphabet"))
		if err != nil {
			t.Fatalf("Failed to compare binary: %v", err)
		}
		if result <= 0 {
			t.Errorf("Expected positive (greater), got %d", result)
		}

		result, err = tower.CompareBinary(key, []byte("zebra"))
		if err != nil {
			t.Fatalf("Failed to compare binary: %v", err)
		}
		if result >= 0 {
			t.Errorf("Expected negative (less), got %d", result)
		}
	})

	// Test bitwise operations
	t.Run("BitwiseOperations", func(t *testing.T) {
		key := "test:binary:bitwise"
		testData := []byte{0xFF, 0xAA, 0x55, 0x00}

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		// Test AndBinary
		mask := []byte{0x0F, 0xFF, 0xF0, 0xFF}
		result, err := tower.AndBinary(key, mask)
		if err != nil {
			t.Fatalf("Failed to AND binary: %v", err)
		}

		expected := []byte{0x0F, 0xAA, 0x50, 0x00}
		if !bytes.Equal(result, expected) {
			t.Errorf("AND operation: expected %v, got %v", expected, result)
		}

		// Test OrBinary
		err = tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to reset binary: %v", err)
		}

		orMask := []byte{0x0F, 0x55, 0xAA, 0xFF}
		result, err = tower.OrBinary(key, orMask)
		if err != nil {
			t.Fatalf("Failed to OR binary: %v", err)
		}

		expected = []byte{0xFF, 0xFF, 0xFF, 0xFF}
		if !bytes.Equal(result, expected) {
			t.Errorf("OR operation: expected %v, got %v", expected, result)
		}

		// Test XorBinary
		err = tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to reset binary: %v", err)
		}

		xorMask := []byte{0xFF, 0xFF, 0xFF, 0xFF}
		result, err = tower.XorBinary(key, xorMask)
		if err != nil {
			t.Fatalf("Failed to XOR binary: %v", err)
		}

		expected = []byte{0x00, 0x55, 0xAA, 0xFF}
		if !bytes.Equal(result, expected) {
			t.Errorf("XOR operation: expected %v, got %v", expected, result)
		}
	})

	// Test ReverseBinary
	t.Run("ReverseBinary", func(t *testing.T) {
		key := "test:binary:reverse"
		testData := []byte("Hello")

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		result, err := tower.ReverseBinary(key)
		if err != nil {
			t.Fatalf("Failed to reverse binary: %v", err)
		}

		expected := []byte("olleH")
		if !bytes.Equal(result, expected) {
			t.Errorf("Expected %v, got %v", expected, result)
		}

		// Verify stored value
		stored, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get reversed binary: %v", err)
		}
		if !bytes.Equal(stored, expected) {
			t.Errorf("Stored value expected %v, got %v", expected, stored)
		}

		// Test reverse again (should get original)
		result, err = tower.ReverseBinary(key)
		if err != nil {
			t.Fatalf("Failed to reverse binary again: %v", err)
		}

		if !bytes.Equal(result, testData) {
			t.Errorf("Double reverse: expected %v, got %v", testData, result)
		}
	})

	// Test large binary data
	t.Run("LargeBinaryData", func(t *testing.T) {
		key := "test:binary:large"

		// Create 1MB of test data
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		err := tower.SetBinary(key, largeData)
		if err != nil {
			t.Fatalf("Failed to set large binary: %v", err)
		}

		retrieved, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get large binary: %v", err)
		}

		if !bytes.Equal(retrieved, largeData) {
			t.Error("Large binary data mismatch")
		}

		// Test length
		length, err := tower.GetBinaryLength(key)
		if err != nil {
			t.Fatalf("Failed to get large binary length: %v", err)
		}

		if length != len(largeData) {
			t.Errorf("Expected length %d, got %d", len(largeData), length)
		}

		// Test appending to large data
		appendData := []byte("appended")
		result, err := tower.AppendBinary(key, appendData)
		if err != nil {
			t.Fatalf("Failed to append to large binary: %v", err)
		}

		expectedLength := len(largeData) + len(appendData)
		if len(result) != expectedLength {
			t.Errorf("Expected result length %d, got %d", expectedLength, len(result))
		}
	})

	// Test binary search and contains operations
	t.Run("BinarySearchAndContains", func(t *testing.T) {
		key := "test:binary:search"
		testData := []byte("Hello, World! This is a test string for binary search.")

		err := tower.SetBinary(key, testData)
		if err != nil {
			t.Fatalf("Failed to set binary: %v", err)
		}

		// Test ContainsBinary
		contains, err := tower.ContainsBinary(key, []byte("World"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if !contains {
			t.Error("Expected to contain 'World'")
		}

		contains, err = tower.ContainsBinary(key, []byte("NotFound"))
		if err != nil {
			t.Fatalf("Failed to check contains: %v", err)
		}
		if contains {
			t.Error("Expected not to contain 'NotFound'")
		}

		// Test IndexBinary
		index, err := tower.GetBinaryIndex(key, []byte("World"))
		if err != nil {
			t.Fatalf("Failed to get index: %v", err)
		}
		expectedIndex := bytes.Index(testData, []byte("World"))
		if index != expectedIndex {
			t.Errorf("Expected index %d, got %d", expectedIndex, index)
		}

		// Test with non-existent substring
		index, err = tower.GetBinaryIndex(key, []byte("NotFound"))
		if err != nil {
			t.Fatalf("Failed to get index for non-existent: %v", err)
		}
		if index != -1 {
			t.Errorf("Expected index -1 for non-existent, got %d", index)
		}
	})

	// Test chained binary operations
	t.Run("ChainedBinaryOperations", func(t *testing.T) {
		key := "test:binary:chained"

		// Start with empty
		err := tower.SetBinary(key, []byte{})
		if err != nil {
			t.Fatalf("Failed to set empty binary: %v", err)
		}

		// Chain operations: append "Hello" -> prepend "Hi, " -> append "!"
		result, err := tower.AppendBinary(key, []byte("Hello"))
		if err != nil {
			t.Fatalf("Failed to append 'Hello': %v", err)
		}
		if !bytes.Equal(result, []byte("Hello")) {
			t.Errorf("After append 'Hello': expected 'Hello', got %s", result)
		}

		result, err = tower.PrependBinary(key, []byte("Hi, "))
		if err != nil {
			t.Fatalf("Failed to prepend 'Hi, ': %v", err)
		}
		if !bytes.Equal(result, []byte("Hi, Hello")) {
			t.Errorf("After prepend 'Hi, ': expected 'Hi, Hello', got %s", result)
		}

		result, err = tower.AppendBinary(key, []byte("!"))
		if err != nil {
			t.Fatalf("Failed to append '!': %v", err)
		}
		if !bytes.Equal(result, []byte("Hi, Hello!")) {
			t.Errorf("After append '!': expected 'Hi, Hello!', got %s", result)
		}

		// Verify final stored value
		final, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get final binary: %v", err)
		}
		if !bytes.Equal(final, []byte("Hi, Hello!")) {
			t.Errorf("Final stored value: expected 'Hi, Hello!', got %s", final)
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:binary:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetBinary(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.AppendBinary(nonExistentKey, []byte("test"))
		if err == nil {
			t.Error("Expected error when appending to non-existent key")
		}

		_, err = tower.GetBinaryLength(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting length of non-existent key")
		}

		_, err = tower.CompareBinaryEqual(nonExistentKey, []byte("test"))
		if err == nil {
			t.Error("Expected error when comparing non-existent key")
		}
	})

	// Test nil and empty byte handling
	t.Run("NilAndEmptyBytes", func(t *testing.T) {
		key := "test:binary:nil_empty"

		// Test with nil bytes (should be treated as empty)
		err := tower.SetBinary(key, nil)
		if err != nil {
			t.Fatalf("Failed to set nil binary: %v", err)
		}

		retrieved, err := tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get nil binary: %v", err)
		}

		// nil should be stored as empty slice
		if len(retrieved) != 0 {
			t.Errorf("Expected empty slice for nil input, got %v", retrieved)
		}

		// Test with empty slice
		err = tower.SetBinary(key, []byte{})
		if err != nil {
			t.Fatalf("Failed to set empty binary: %v", err)
		}

		retrieved, err = tower.GetBinary(key)
		if err != nil {
			t.Fatalf("Failed to get empty binary: %v", err)
		}

		if len(retrieved) != 0 {
			t.Errorf("Expected empty slice, got %v", retrieved)
		}

		// Test operations with empty data
		_, err = tower.AppendBinary(key, []byte("test"))
		if err != nil {
			t.Fatalf("Failed to append to empty binary: %v", err)
		}

		_, err = tower.PrependBinary(key, []byte("prefix"))
		if err != nil {
			t.Fatalf("Failed to prepend to binary: %v", err)
		}
	})
}
