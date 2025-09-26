package op

import (
	"fmt"
	"strings"
)

func (op *Operator) SetString(key string, value string) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetString(value); err != nil {
		return fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetString(key string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return value, nil
}

// String manipulation operations
func (op *Operator) AppendString(key string, suffix string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	newValue := current + suffix
	if err := df.SetString(newValue); err != nil {
		return "", fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) PrependString(key string, prefix string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	newValue := prefix + current
	if err := df.SetString(newValue); err != nil {
		return "", fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) ReplaceString(key string, old, new string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	newValue := strings.ReplaceAll(current, old, new)
	if err := df.SetString(newValue); err != nil {
		return "", fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// Search operations
func (op *Operator) ContainsString(key string, substr string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.Contains(current, substr), nil
}

func (op *Operator) StartsWithString(key string, prefix string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.HasPrefix(current, prefix), nil
}

func (op *Operator) EndsWithString(key string, suffix string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.HasSuffix(current, suffix), nil
}

// Length and substring operations
func (op *Operator) GetStringLength(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return len(current), nil
}

func (op *Operator) GetStringSubstring(key string, start, length int) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	runes := []rune(current)
	if start < 0 || start >= len(runes) {
		return "", fmt.Errorf("start index out of range")
	}

	end := start + length
	if end > len(runes) {
		end = len(runes)
	}

	return string(runes[start:end]), nil
}

// Comparison operations
func (op *Operator) CompareString(key string, other string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.Compare(current, other), nil
}

func (op *Operator) CompareStringEqual(key string, other string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return current == other, nil
}

// Conversion operations
func (op *Operator) UpperString(key string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	newValue := strings.ToUpper(current)
	if err := df.SetString(newValue); err != nil {
		return "", fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) LowerString(key string) (string, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	newValue := strings.ToLower(current)
	if err := df.SetString(newValue); err != nil {
		return "", fmt.Errorf("failed to set string value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

