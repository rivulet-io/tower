package op

import (
	"testing"

	"github.com/google/uuid"
	"github.com/rivulet-io/tower/util/size"
)

func TestUUIDOperations(t *testing.T) {
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

	// Test SetUUID and GetUUID
	t.Run("SetUUID_GetUUID", func(t *testing.T) {
		key := "test:uuid"
		testUUID := uuid.New()

		err := tower.SetUUID(key, &testUUID)
		if err != nil {
			t.Fatalf("Failed to set UUID: %v", err)
		}

		retrieved, err := tower.GetUUID(key)
		if err != nil {
			t.Fatalf("Failed to get UUID: %v", err)
		}

		if retrieved.String() != testUUID.String() {
			t.Errorf("Expected %v, got %v", testUUID, *retrieved)
		}
	})

	// Test GenerateUUID
	t.Run("GenerateUUID", func(t *testing.T) {
		key := "test:uuid:generate"

		generatedUUID, err := tower.GenerateUUID(key)
		if err != nil {
			t.Fatalf("Failed to generate UUID: %v", err)
		}

		if generatedUUID == nil {
			t.Fatal("Generated UUID is nil")
		}

		// Verify it was stored
		retrieved, err := tower.GetUUID(key)
		if err != nil {
			t.Fatalf("Failed to get generated UUID: %v", err)
		}

		if retrieved.String() != generatedUUID.String() {
			t.Errorf("Expected %v, got %v", *generatedUUID, *retrieved)
		}

		// Verify it's a valid UUID v7 (time-based)
		if generatedUUID.Version() != 7 {
			t.Errorf("Expected UUID version 7, got %v", generatedUUID.Version())
		}
	})

	// Test EqualUUID
	t.Run("EqualUUID", func(t *testing.T) {
		key := "test:uuid:equal"
		testUUID := uuid.New()
		differentUUID := uuid.New()

		err := tower.SetUUID(key, &testUUID)
		if err != nil {
			t.Fatalf("Failed to set UUID: %v", err)
		}

		// Test equality with same UUID
		equal, err := tower.CompareUUIDEqual(key, &testUUID)
		if err != nil {
			t.Fatalf("Failed to check UUID equality: %v", err)
		}
		if !equal {
			t.Error("Expected UUIDs to be equal")
		}

		// Test inequality with different UUID
		equal, err = tower.CompareUUIDEqual(key, &differentUUID)
		if err != nil {
			t.Fatalf("Failed to check UUID inequality: %v", err)
		}
		if equal {
			t.Error("Expected UUIDs to be different")
		}
	})

	// Test CompareUUID
	t.Run("CompareUUID", func(t *testing.T) {
		key := "test:uuid:compare"
		uuid1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		uuid2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
		uuid3 := uuid.MustParse("00000000-0000-0000-0000-000000000001") // Same as uuid1

		err := tower.SetUUID(key, &uuid1)
		if err != nil {
			t.Fatalf("Failed to set UUID: %v", err)
		}

		// Compare with larger UUID (should return -1)
		result, err := tower.CompareUUID(key, &uuid2)
		if err != nil {
			t.Fatalf("Failed to compare UUID: %v", err)
		}
		if result != -1 {
			t.Errorf("Expected -1 (less), got %d", result)
		}

		// Compare with same UUID (should return 0)
		result, err = tower.CompareUUID(key, &uuid3)
		if err != nil {
			t.Fatalf("Failed to compare UUID: %v", err)
		}
		if result != 0 {
			t.Errorf("Expected 0 (equal), got %d", result)
		}

		// Set to larger UUID and compare with smaller (should return 1)
		err = tower.SetUUID(key, &uuid2)
		if err != nil {
			t.Fatalf("Failed to set larger UUID: %v", err)
		}

		result, err = tower.CompareUUID(key, &uuid1)
		if err != nil {
			t.Fatalf("Failed to compare UUID: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1 (greater), got %d", result)
		}
	})

	// Test UUID validation
	t.Run("UUIDValidation", func(t *testing.T) {
		key := "test:uuid:validation"

		// Test with valid UUID
		validUUID := uuid.New()
		err := tower.SetUUID(key, &validUUID)
		if err != nil {
			t.Fatalf("Failed to set valid UUID: %v", err)
		}

		isValid, err := tower.ValidateUUID(key)
		if err != nil {
			t.Fatalf("Failed to check UUID validity: %v", err)
		}
		if !isValid {
			t.Error("Expected UUID to be valid")
		}

		isNil, err := tower.CheckUUIDNil(key)
		if err != nil {
			t.Fatalf("Failed to check if UUID is nil: %v", err)
		}
		if isNil {
			t.Error("Expected UUID not to be nil")
		}

		// Test with nil UUID
		nilUUID := uuid.Nil
		err = tower.SetUUID(key, &nilUUID)
		if err != nil {
			t.Fatalf("Failed to set nil UUID: %v", err)
		}

		isValid, err = tower.ValidateUUID(key)
		if err != nil {
			t.Fatalf("Failed to check nil UUID validity: %v", err)
		}
		if isValid {
			t.Error("Expected nil UUID to be invalid")
		}

		isNil, err = tower.CheckUUIDNil(key)
		if err != nil {
			t.Fatalf("Failed to check if UUID is nil: %v", err)
		}
		if !isNil {
			t.Error("Expected UUID to be nil")
		}
	})

	// Test UUID conversion operations
	t.Run("UUIDConversion", func(t *testing.T) {
		key := "test:uuid:conversion"
		testUUID := uuid.New()

		err := tower.SetUUID(key, &testUUID)
		if err != nil {
			t.Fatalf("Failed to set UUID: %v", err)
		}

		// Test UUIDToString
		uuidString, err := tower.ConvertUUIDToString(key)
		if err != nil {
			t.Fatalf("Failed to convert UUID to string: %v", err)
		}

		if uuidString != testUUID.String() {
			t.Errorf("Expected %s, got %s", testUUID.String(), uuidString)
		}

		// Test StringToUUID
		newKey := "test:uuid:conversion:from_string"
		convertedUUID, err := tower.ConvertStringToUUID(newKey, uuidString)
		if err != nil {
			t.Fatalf("Failed to convert string to UUID: %v", err)
		}

		if convertedUUID.String() != testUUID.String() {
			t.Errorf("Expected %v, got %v", testUUID, *convertedUUID)
		}

		// Verify it was stored correctly
		retrieved, err := tower.GetUUID(newKey)
		if err != nil {
			t.Fatalf("Failed to get converted UUID: %v", err)
		}

		if retrieved.String() != testUUID.String() {
			t.Errorf("Expected %v, got %v", testUUID, *retrieved)
		}

		// Test invalid string conversion
		invalidKey := "test:uuid:conversion:invalid"
		_, err = tower.ConvertStringToUUID(invalidKey, "invalid-uuid-string")
		if err == nil {
			t.Error("Expected error when converting invalid UUID string")
		}
	})

	// Test UUID information operations
	t.Run("UUIDInformation", func(t *testing.T) {
		key := "test:uuid:info"

		// Test with different UUID versions
		uuid4 := uuid.New() // Version 4 (random)
		err := tower.SetUUID(key, &uuid4)
		if err != nil {
			t.Fatalf("Failed to set UUID v4: %v", err)
		}

		version, err := tower.GetUUIDVersion(key)
		if err != nil {
			t.Fatalf("Failed to get UUID version: %v", err)
		}
		if version != 4 {
			t.Errorf("Expected version 4, got %v", version)
		}

		variant, err := tower.GetUUIDVariant(key)
		if err != nil {
			t.Fatalf("Failed to get UUID variant: %v", err)
		}
		if variant != uuid.RFC4122 {
			t.Errorf("Expected RFC4122 variant, got %v", variant)
		}

		// Test with UUID v7
		uuidV7, err := uuid.NewV7()
		if err != nil {
			t.Fatalf("Failed to create UUID v7: %v", err)
		}

		err = tower.SetUUID(key, &uuidV7)
		if err != nil {
			t.Fatalf("Failed to set UUID v7: %v", err)
		}

		version, err = tower.GetUUIDVersion(key)
		if err != nil {
			t.Fatalf("Failed to get UUID v7 version: %v", err)
		}
		if version != 7 {
			t.Errorf("Expected version 7, got %v", version)
		}
	})

	// Test conditional UUID setting operations
	t.Run("ConditionalUUIDSetting", func(t *testing.T) {
		key := "test:uuid:conditional"

		// Test SetUUIDIfNil with nil UUID
		nilUUID := uuid.Nil
		err := tower.SetUUID(key, &nilUUID)
		if err != nil {
			t.Fatalf("Failed to set nil UUID: %v", err)
		}

		newUUID, err := tower.SetUUIDIfNil(key)
		if err != nil {
			t.Fatalf("Failed to SetUUIDIfNil: %v", err)
		}

		if newUUID.String() == uuid.Nil.String() {
			t.Error("Expected new UUID to be generated, but got nil UUID")
		}

		// Test SetUUIDIfNil with non-nil UUID (should not change)
		existingUUID, err := tower.GetUUID(key)
		if err != nil {
			t.Fatalf("Failed to get existing UUID: %v", err)
		}

		unchangedUUID, err := tower.SetUUIDIfNil(key)
		if err != nil {
			t.Fatalf("Failed to SetUUIDIfNil on non-nil: %v", err)
		}

		if unchangedUUID.String() != existingUUID.String() {
			t.Errorf("Expected UUID to remain unchanged: %v, got %v", *existingUUID, *unchangedUUID)
		}

		// Test SetUUIDIfEqual
		expectedUUID := *existingUUID
		newValue := uuid.New()
		result, err := tower.SetUUIDIfEqual(key, &expectedUUID, &newValue)
		if err != nil {
			t.Fatalf("Failed to SetUUIDIfEqual: %v", err)
		}

		if result.String() != newValue.String() {
			t.Errorf("Expected %v, got %v", newValue, *result)
		}

		// Test SetUUIDIfEqual with wrong expected value (should not change)
		wrongExpected := uuid.New()
		anotherNew := uuid.New()
		result, err = tower.SetUUIDIfEqual(key, &wrongExpected, &anotherNew)
		if err != nil {
			t.Fatalf("Failed to SetUUIDIfEqual: %v", err)
		}

		if result.String() != newValue.String() {
			t.Errorf("Expected %v (unchanged), got %v", newValue, *result)
		}
	})

	// Test with specific UUID formats
	t.Run("SpecificUUIDFormats", func(t *testing.T) {
		key := "test:uuid:formats"

		testCases := []struct {
			name     string
			uuidStr  string
			expected uuid.UUID
		}{
			{
				name:     "Standard format",
				uuidStr:  "123e4567-e89b-12d3-a456-426614174000",
				expected: uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			},
			{
				name:     "Nil UUID",
				uuidStr:  "00000000-0000-0000-0000-000000000000",
				expected: uuid.Nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				convertedUUID, err := tower.ConvertStringToUUID(key, tc.uuidStr)
				if err != nil {
					t.Fatalf("Failed to convert UUID string: %v", err)
				}

				if convertedUUID.String() != tc.expected.String() {
					t.Errorf("Expected %v, got %v", tc.expected, *convertedUUID)
				}

				// Test round-trip conversion
				backToString, err := tower.ConvertUUIDToString(key)
				if err != nil {
					t.Fatalf("Failed to convert UUID back to string: %v", err)
				}

				if backToString != tc.uuidStr {
					t.Errorf("Expected %s, got %s", tc.uuidStr, backToString)
				}
			})
		}
	})

	// Test error cases
	t.Run("ErrorCases", func(t *testing.T) {
		nonExistentKey := "test:uuid:nonexistent"

		// Test getting non-existent key
		_, err := tower.GetUUID(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting non-existent key")
		}

		// Test operations on non-existent key
		_, err = tower.CompareUUIDEqual(nonExistentKey, &uuid.Nil)
		if err == nil {
			t.Error("Expected error when comparing non-existent key")
		}

		_, err = tower.ConvertUUIDToString(nonExistentKey)
		if err == nil {
			t.Error("Expected error when converting non-existent key to string")
		}

		_, err = tower.ValidateUUID(nonExistentKey)
		if err == nil {
			t.Error("Expected error when checking validity of non-existent key")
		}
	})
}

