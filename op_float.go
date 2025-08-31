package tower

import (
	"fmt"
	"math"
)

func (t *Tower) SetFloat(key string, value float64) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetFloat(value); err != nil {
		return fmt.Errorf("failed to set float value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetFloat(key string) (float64, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Float()
	if err != nil {
		return 0, fmt.Errorf("failed to get float value for key %s: %w", key, err)
	}

	return value, nil
}

func (t *Tower) AddFloat(key string, delta float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SubFloat(key string, delta float64) (float64, error) {
	return t.AddFloat(key, -delta)
}

func (t *Tower) MulFloat(key string, factor float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) DivFloat(key string, divisor float64) (float64, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("division by zero")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) NegFloat(key string) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) AbsFloat(key string) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SwapFloat(key string, newValue float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return current, nil
}

// 비교 연산
func (t *Tower) CompareFloat(key string, value float64) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
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

func (t *Tower) SetFloatIfGreater(key string, value float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetFloatIfLess(key string, value float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetFloatIfEqual(key string, expected, newValue float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}

// 범위 및 제한 연산
func (t *Tower) ClampFloat(key string, min, max float64) (float64, error) {
	if min > max {
		return 0, fmt.Errorf("min cannot be greater than max")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (t *Tower) MinFloat(key string, value float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (t *Tower) MaxFloat(key string, value float64) (float64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}
