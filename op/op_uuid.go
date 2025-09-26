package op

import (
	"fmt"

	"github.com/google/uuid"
)

func (op *Operator) SetUUID(key string, value *uuid.UUID) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetUUID(value); err != nil {
		return fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetUUID(key string) (*uuid.UUID, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.UUID()
	if err != nil {
		return nil, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return value, nil
}

// UUID generation operations
func (op *Operator) GenerateUUID(key string) (*uuid.UUID, error) {
	unlock := op.lock(key)
	defer unlock()

	newUUID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID v7: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetUUID(&newUUID); err != nil {
		return nil, fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return &newUUID, nil
}

// Comparison operations
func (op *Operator) CompareUUIDEqual(key string, other *uuid.UUID) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String() == other.String(), nil
}

func (op *Operator) CompareUUID(key string, other *uuid.UUID) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

// Validation operations
func (op *Operator) ValidateUUID(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current != nil && current.String() != uuid.Nil.String(), nil
}

func (op *Operator) CheckUUIDNil(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return false, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String() == uuid.Nil.String(), nil
}

// Conversion operations
func (op *Operator) ConvertUUIDToString(key string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return "", fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.String(), nil
}

func (op *Operator) ConvertStringToUUID(key string, uuidStr string) (*uuid.UUID, error) {
	unlock := op.lock(key)
	defer unlock()

	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse UUID string: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetUUID(&parsedUUID); err != nil {
		return nil, fmt.Errorf("failed to set UUID value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return &parsedUUID, nil
}

// UUID information operations
func (op *Operator) GetUUIDVersion(key string) (uuid.Version, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return 0, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.Version(), nil
}

func (op *Operator) GetUUIDVariant(key string) (uuid.Variant, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.UUID()
	if err != nil {
		return 0, fmt.Errorf("failed to get UUID value for key %s: %w", key, err)
	}

	return current.Variant(), nil
}

// Conditional set operations
func (op *Operator) SetUUIDIfNil(key string) (*uuid.UUID, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return nil, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return &newUUID, nil
	}

	return current, nil
}

func (op *Operator) SetUUIDIfEqual(key string, expected, newValue *uuid.UUID) (*uuid.UUID, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return nil, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}
