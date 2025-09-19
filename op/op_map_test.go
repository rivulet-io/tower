package op

import (
	"testing"
)

func TestMapBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_map"

	// 맵 생성
	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	// 맵 존재 확인
	exists, err := tower.MapExists(key)
	if err != nil {
		t.Fatalf("Failed to check map existence: %v", err)
	}
	if !exists {
		t.Error("Expected map to exist")
	}

	// 초기 길이 확인
	length, err := tower.MapLength(key)
	if err != nil {
		t.Fatalf("Failed to get map length: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected empty map length 0, got %d", length)
	}

	// 맵 삭제
	if err := tower.DeleteMap(key); err != nil {
		t.Fatalf("Failed to delete map: %v", err)
	}

	// 삭제 후 존재 확인
	exists, err = tower.MapExists(key)
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

	// 맵 생성
	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	// 다양한 타입의 필드 설정 및 조회
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

	// 설정 테스트
	for _, tc := range testCases {
		if err := tower.MapSet(key, tc.field, tc.value); err != nil {
			fieldStr, _ := tc.field.String()
			t.Fatalf("Failed to set field %s: %v", fieldStr, err)
		}
	}

	// 길이 확인
	length, err := tower.MapLength(key)
	if err != nil {
		t.Fatalf("Failed to get map length: %v", err)
	}
	if length != int64(len(testCases)) {
		t.Errorf("Expected length %d, got %d", len(testCases), length)
	}

	// 조회 테스트
	for _, tc := range testCases {
		value, err := tower.MapGet(key, tc.field)
		if err != nil {
			fieldStr, _ := tc.field.String()
			t.Fatalf("Failed to get field %s: %v", fieldStr, err)
		}

		// 바이너리 데이터는 별도로 비교
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
			// 다른 타입들은 직접 비교
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

	// 존재하지 않는 맵에 대한 작업 테스트
	_, err := tower.MapExists(key)
	if err != nil {
		t.Fatalf("MapExists should not error for non-existent map: %v", err)
	}

	err = tower.MapSet(key, PrimitiveString("field"), PrimitiveString("value"))
	if err == nil {
		t.Error("Expected error when setting field in non-existent map")
	}

	_, err = tower.MapGet(key, PrimitiveString("field"))
	if err == nil {
		t.Error("Expected error when getting field from non-existent map")
	}

	// 맵 생성
	if err := tower.CreateMap(key); err != nil {
		t.Fatalf("Failed to create map: %v", err)
	}

	// 중복 생성 시도
	if err := tower.CreateMap(key); err == nil {
		t.Error("Expected error when creating map that already exists")
	}

	// 존재하지 않는 필드 조회
	_, err = tower.MapGet(key, PrimitiveString("nonexistent_field"))
	if err == nil {
		t.Error("Expected error when getting non-existent field")
	}
}
