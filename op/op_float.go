package op

import (
	"fmt"
	"math"
)

func (op *Operator) SetFloat(key string, value float64) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetFloat(value); err != nil {
		return fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetFloat(key string) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	return value, nil
}

func (op *Operator) AddFloat(key string, delta float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current + delta
	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SubFloat(key string, delta float64) (float64, error) {
	return op.AddFloat(key, -delta)
}

func (op *Operator) MulFloat(key string, factor float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current * factor
	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) DivFloat(key string, divisor float64) (float64, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("division by zero")
	}

	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current / divisor
	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) NegFloat(key string) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := -current
	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) AbsFloat(key string) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := math.Abs(current)
	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SwapFloat(key string, newValue float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	if err := df.SetFloat(newValue); err != nil {
		return 0, fmt.Errorf("failed to set float value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return current, nil
}

// 비교 연산
func (op *Operator) CompareFloat(key string, value float64) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	if current < value {
		return -1, nil
	} else if current > value {
		return 1, nil
	}
	return 0, nil
}

func (op *Operator) SetFloatIfGreater(key string, value float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	if value > current {
		if err := df.SetFloat(value); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetFloatIfLess(key string, value float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	if value < current {
		if err := df.SetFloat(value); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetFloatIfEqual(key string, expected, newValue float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	if current == expected {
		if err := df.SetFloat(newValue); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}

// 범위 및 제한 연산
func (op *Operator) ClampFloat(key string, min, max float64) (float64, error) {
	if min > max {
		return 0, fmt.Errorf("min cannot be greater than max")
	}

	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current
	if newValue < min {
		newValue = min
	} else if newValue > max {
		newValue = max
	}

	if newValue != current {
		if err := df.SetFloat(newValue); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (op *Operator) MinFloat(key string, value float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current
	if value < current {
		newValue = value
	}

	if newValue != current {
		if err := df.SetFloat(newValue); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (op *Operator) MaxFloat(key string, value float64) (float64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	newValue := current
	if value > current {
		newValue = value
	}

	if newValue != current {
		if err := df.SetFloat(newValue); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}
