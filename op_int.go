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
	unlock := t.rlock(key)
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
