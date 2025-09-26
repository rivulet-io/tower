package op

import (
	"testing"
)

func TestMapBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_map"

	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	exists, err := tower.ExistsMap(key)
	if err != nil {
		t.Fatalf("Failed to check map existence: %v", err)
	}
	if !exists {
		t.Error("Expected map to exist")
	}

	length, err := tower.GetMapLength(key)
	if err != nil {
		t.Fatalf("Failed to get map length: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected empty map length 0, got %d", length)
	}

	if err := tower.DeleteMap(key); err != nil {
		t.Fatalf("Failed to delete map: %v", err)
	}

	exists, err = tower.ExistsMap(key)
	if err != nil {
		t.Fatalf("Failed to check map existence after delete: %v", err)
	}
	if exists {
		t.Error("Expected map to not exist after deletion")
	}
}

func TestMapSetAndGet(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_map"

	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	testCases := []struct {
		field PrimitiveData
		value PrimitiveData
	}{
		{PrimitiveString("string_field"), PrimitiveString("hello_world")},
		{PrimitiveString("int_field"), PrimitiveInt(42)},
		{PrimitiveString("float_field"), PrimitiveFloat(3.14)},
		{PrimitiveString("bool_field"), PrimitiveBool(true)},
		{PrimitiveString("binary_field"), PrimitiveBinary([]byte("binary_data"))},
	}

	for _, tc := range testCases {
		if err := tower.SetMapKey(key, tc.field, tc.value); err != nil {
			fieldStr, _ := tc.field.String()
			t.Fatalf("Failed to set field %s: %v", fieldStr, err)
		}
	}

	length, err := tower.GetMapLength(key)
	if err != nil {
		t.Fatalf("Failed to get map length: %v", err)
	}
	if length != int64(len(testCases)) {
		t.Errorf("Expected length %d, got %d", len(testCases), length)
	}

	for _, tc := range testCases {
		value, err := tower.GetMapKey(key, tc.field)
		if err != nil {
			fieldStr, _ := tc.field.String()
			t.Fatalf("Failed to get field %s: %v", fieldStr, err)
		}

		if expectedBin, err := tc.value.Binary(); err == nil {
			if retrievedBin, err := value.Binary(); err == nil {
				if string(expectedBin) != string(retrievedBin) {
					fieldStr, _ := tc.field.String()
					t.Errorf("Binary data mismatch for field %s", fieldStr)
				}
			} else {
				fieldStr, _ := tc.field.String()
				t.Errorf("Expected binary data for field %s", fieldStr)
			}
		} else {
			if value.Type() != tc.value.Type() {
				fieldStr, _ := tc.field.String()
				t.Errorf("Type mismatch for field %s", fieldStr)
				continue
			}

			switch tc.value.Type() {
			case TypeString:
				expectedStr, _ := tc.value.String()
				actualStr, _ := value.String()
				if expectedStr != actualStr {
					fieldStr, _ := tc.field.String()
					t.Errorf("Expected %v for field %s, got %v", expectedStr, fieldStr, actualStr)
				}
			case TypeInt:
				expectedInt, _ := tc.value.Int()
				actualInt, _ := value.Int()
				if expectedInt != actualInt {
					fieldStr, _ := tc.field.String()
					t.Errorf("Expected %v for field %s, got %v", expectedInt, fieldStr, actualInt)
				}
			case TypeFloat:
				expectedFloat, _ := tc.value.Float()
				actualFloat, _ := value.Float()
				if expectedFloat != actualFloat {
					fieldStr, _ := tc.field.String()
					t.Errorf("Expected %v for field %s, got %v", expectedFloat, fieldStr, actualFloat)
				}
			case TypeBool:
				expectedBool, _ := tc.value.Bool()
				actualBool, _ := value.Bool()
				if expectedBool != actualBool {
					fieldStr, _ := tc.field.String()
					t.Errorf("Expected %v for field %s, got %v", expectedBool, fieldStr, actualBool)
				}
			}
		}
	}
}

func TestMapErrorCases(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_map"

	_, err := tower.ExistsMap(key)
	if err != nil {
		t.Fatalf("MapExists should not error for non-existent map: %v", err)
	}

	err = tower.SetMapKey(key, PrimitiveString("field"), PrimitiveString("value"))
	if err == nil {
		t.Error("Expected error when setting field in non-existent map")
	}

	_, err = tower.GetMapKey(key, PrimitiveString("field"))
	if err == nil {
		t.Error("Expected error when getting field from non-existent map")
	}

	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	if err := tower.CreateMap(key); err == nil {
		t.Error("Expected error when creating map that already exists")
	}

	_, err = tower.GetMapKey(key, PrimitiveString("nonexistent_field"))
	if err == nil {
		t.Error("Expected error when getting non-existent field")
	}
}

