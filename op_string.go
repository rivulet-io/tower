package tower

import (
	"fmt"
	"strings"
)

func (t *Tower) SetString(key string, value string) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetString(value); err != nil {
		return fmt.Errorf("failed to set string value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetString(key string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.String()
	if err != nil {
		return "", fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return value, nil
}

// 문자열 조작 연산
func (t *Tower) AppendString(key string, suffix string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) PrependString(key string, prefix string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) ReplaceString(key string, old, new string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// 검색 연산
func (t *Tower) ContainsString(key string, substr string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.Contains(current, substr), nil
}

func (t *Tower) StartsWithString(key string, prefix string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.HasPrefix(current, prefix), nil
}

func (t *Tower) EndsWithString(key string, suffix string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.HasSuffix(current, suffix), nil
}

// 길이 및 부분 문자열 연산
func (t *Tower) LengthString(key string) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return len(current), nil
}

func (t *Tower) SubstringString(key string, start, length int) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

// 비교 연산
func (t *Tower) CompareString(key string, other string) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return strings.Compare(current, other), nil
}

func (t *Tower) EqualString(key string, other string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.String()
	if err != nil {
		return false, fmt.Errorf("failed to get string value for key %s: %w", key, err)
	}

	return current == other, nil
}

// 변환 연산
func (t *Tower) UpperString(key string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) LowerString(key string) (string, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return "", fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}
