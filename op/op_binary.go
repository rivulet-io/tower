package op

import (
	"bytes"
	"fmt"
)

func (op *Operator) SetBinary(key string, value []byte) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetBinary(value); err != nil {
		return fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetBinary(key string) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return value, nil
}

// 바이트 조작 연산
func (op *Operator) AppendBinary(key string, data []byte) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	newValue := append(current, data...)
	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) PrependBinary(key string, data []byte) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	newValue := append(data, current...)
	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// 길이 및 부분 바이트 연산
func (op *Operator) LengthBinary(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return 0, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return len(current), nil
}

func (op *Operator) SubBinary(key string, start, length int) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	if start < 0 || start >= len(current) {
		return nil, fmt.Errorf("start index out of range")
	}

	end := start + length
	if end > len(current) {
		end = len(current)
	}

	result := make([]byte, end-start)
	copy(result, current[start:end])
	return result, nil
}

// 비교 연산
func (op *Operator) EqualBinary(key string, other []byte) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return false, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return bytes.Equal(current, other), nil
}

func (op *Operator) CompareBinary(key string, other []byte) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return 0, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return bytes.Compare(current, other), nil
}

// 비트 연산
func (op *Operator) AndBinary(key string, mask []byte) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	minLen := len(current)
	if len(mask) < minLen {
		minLen = len(mask)
	}

	newValue := make([]byte, len(current))
	copy(newValue, current)

	for i := 0; i < minLen; i++ {
		newValue[i] &= mask[i]
	}

	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) OrBinary(key string, mask []byte) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	minLen := len(current)
	if len(mask) < minLen {
		minLen = len(mask)
	}

	newValue := make([]byte, len(current))
	copy(newValue, current)

	for i := 0; i < minLen; i++ {
		newValue[i] |= mask[i]
	}

	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) XorBinary(key string, mask []byte) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	minLen := len(current)
	if len(mask) < minLen {
		minLen = len(mask)
	}

	newValue := make([]byte, len(current))
	copy(newValue, current)

	for i := 0; i < minLen; i++ {
		newValue[i] ^= mask[i]
	}

	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// 검색 연산
func (op *Operator) ContainsBinary(key string, sub []byte) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return false, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return bytes.Contains(current, sub), nil
}

func (op *Operator) IndexBinary(key string, sub []byte) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return -1, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return -1, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	return bytes.Index(current, sub), nil
}

// 변환 연산
func (op *Operator) ReverseBinary(key string) ([]byte, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Binary()
	if err != nil {
		return nil, fmt.Errorf("failed to get binary value for key %s: %w", key, err)
	}

	newValue := make([]byte, len(current))
	copy(newValue, current)

	for i, j := 0, len(newValue)-1; i < j; i, j = i+1, j-1 {
		newValue[i], newValue[j] = newValue[j], newValue[i]
	}

	if err := df.SetBinary(newValue); err != nil {
		return nil, fmt.Errorf("failed to set binary value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}
