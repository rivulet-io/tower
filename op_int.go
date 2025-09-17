package tower

import "fmt"

func (t *Tower) SetInt(key string, value int64) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetInt(value); err != nil {
		return fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetInt(key string) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	return value, nil
}

func (t *Tower) AddInt(key string, delta int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current + delta
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SubInt(key string, delta int64) (int64, error) {
	return t.AddInt(key, -delta)
}

func (t *Tower) IncInt(key string) (int64, error) {
	return t.AddInt(key, 1)
}

func (t *Tower) DecInt(key string) (int64, error) {
	return t.SubInt(key, 1)
}

func (t *Tower) MulInt(key string, factor int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current * factor
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) DivInt(key string, divisor int64) (int64, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("division by zero")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current / divisor
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) ModInt(key string, modulus int64) (int64, error) {
	if modulus == 0 {
		return 0, fmt.Errorf("modulus by zero")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current % modulus
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) NegInt(key string) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := -current
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) AbsInt(key string) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current
	if newValue < 0 {
		newValue = -newValue
	}
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SwapInt(key string, newValue int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return current, nil
}

// 비교 연산
func (t *Tower) CompareInt(key string, value int64) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	if current < value {
		return -1, nil
	} else if current > value {
		return 1, nil
	}
	return 0, nil
}

// 조건부 설정 연산
func (t *Tower) SetIntIfGreater(key string, value int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	if value > current {
		if err := df.SetInt(value); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetIntIfLess(key string, value int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	if value < current {
		if err := df.SetInt(value); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetIntIfEqual(key string, expected, newValue int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	if current == expected {
		if err := df.SetInt(newValue); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}

// 범위 및 제한 연산
func (t *Tower) ClampInt(key string, min, max int64) (int64, error) {
	if min > max {
		return 0, fmt.Errorf("min cannot be greater than max")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current
	if newValue < min {
		newValue = min
	} else if newValue > max {
		newValue = max
	}

	if newValue != current {
		if err := df.SetInt(newValue); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (t *Tower) MinInt(key string, value int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current
	if value < current {
		newValue = value
	}

	if newValue != current {
		if err := df.SetInt(newValue); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

func (t *Tower) MaxInt(key string, value int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current
	if value > current {
		newValue = value
	}

	if newValue != current {
		if err := df.SetInt(newValue); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
	}

	return newValue, nil
}

// 비트 연산
func (t *Tower) AndInt(key string, mask int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current & mask
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) OrInt(key string, mask int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current | mask
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) XorInt(key string, mask int64) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current ^ mask
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) ShiftLeftInt(key string, bits uint) (int64, error) {
	if bits > 63 {
		return 0, fmt.Errorf("shift bits cannot be greater than 63")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current << bits
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) ShiftRightInt(key string, bits uint) (int64, error) {
	if bits > 63 {
		return 0, fmt.Errorf("shift bits cannot be greater than 63")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Int()
	if err != nil {
		return 0, fmt.Errorf("failed to get int value for key %s: %w", key, err)
	}

	newValue := current >> bits
	if err := df.SetInt(newValue); err != nil {
		return 0, fmt.Errorf("failed to set int value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}
