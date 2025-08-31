package tower

import (
	"fmt"

	"github.com/google/uuid"
)

func (t *Tower) SetUUID(key string, value *uuid.UUID) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetUUID(value); err != nil {
		return fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetUUID(key string) (*uuid.UUID, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.UUID()
	if err != nil {
		return nil, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return value, nil
}

// UUID 생성 연산
func (t *Tower) GenerateUUID(key string) (*uuid.UUID, error) {
	unlock := t.lock(key)
	defer unlock()

	newUUID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID v7: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetUUID(&newUUID); err != nil {
		return nil, fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return &newUUID, nil
}

// 비교 연산
func (t *Tower) EqualUUID(key string, other *uuid.UUID) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String() == other.String(), nil
}

func (t *Tower) CompareUUID(key string, other *uuid.UUID) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return 0, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	currentStr := current.String()
	otherStr := other.String()

	if currentStr < otherStr {
		return -1, nil
	} else if currentStr > otherStr {
		return 1, nil
	}
	return 0, nil
}

// 검증 연산
func (t *Tower) IsValidUUID(key string) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current != nil && current.String() != uuid.Nil.String(), nil
}

func (t *Tower) IsNilUUID(key string) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String() == uuid.Nil.String(), nil
}

// 변환 연산
func (t *Tower) UUIDToString(key string) (string, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return "", fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String(), nil
}

func (t *Tower) StringToUUID(key string, uuidStr string) (*uuid.UUID, error) {
	unlock := t.lock(key)
	defer unlock()

	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UUID string: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetUUID(&parsedUUID); err != nil {
		return nil, fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return &parsedUUID, nil
}

// UUID 정보 연산
func (t *Tower) UUIDVersion(key string) (uuid.Version, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return 0, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.Version(), nil
}

func (t *Tower) UUIDVariant(key string) (uuid.Variant, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return 0, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.Variant(), nil
}

// 조건부 설정 연산
func (t *Tower) SetUUIDIfNil(key string) (*uuid.UUID, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return nil, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	if current.String() == uuid.Nil.String() {
		newUUID := uuid.New()
		if err := df.SetUUID(&newUUID); err != nil {
			return nil, fmt.Errorf("failed to set UUID value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return nil, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return &newUUID, nil
	}

	return current, nil
}

func (t *Tower) SetUUIDIfEqual(key string, expected, newValue *uuid.UUID) (*uuid.UUID, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return nil, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	if current.String() == expected.String() {
		if err := df.SetUUID(newValue); err != nil {
			return nil, fmt.Errorf("failed to set UUID value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return nil, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}
