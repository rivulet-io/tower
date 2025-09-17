package tower

import (
	"fmt"
	"time"
)

func (t *Tower) SetDuration(key string, value time.Duration) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetDuration(value); err != nil {
		return fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetDuration(key string) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	return value, nil
}

func (t *Tower) AddDuration(key string, delta time.Duration) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	newValue := current + delta
	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SubDuration(key string, delta time.Duration) (time.Duration, error) {
	return t.AddDuration(key, -delta)
}

func (t *Tower) MulDuration(key string, factor int64) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	newValue := current * time.Duration(factor)
	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) DivDuration(key string, divisor int64) (time.Duration, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("division by zero")
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	newValue := current / time.Duration(divisor)
	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) NegDuration(key string) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	newValue := -current
	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) AbsDuration(key string) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	newValue := current
	if newValue < 0 {
		newValue = -newValue
	}
	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SwapDuration(key string, newValue time.Duration) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	if err := df.SetDuration(newValue); err != nil {
		return 0, fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return current, nil
}

func (t *Tower) CompareDuration(key string, value time.Duration) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	if current < value {
		return -1, nil
	} else if current > value {
		return 1, nil
	}
	return 0, nil
}

func (t *Tower) SetDurationIfGreater(key string, value time.Duration) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	if value > current {
		if err := df.SetDuration(value); err != nil {
			return 0, fmt.Errorf("failed to set duration value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetDurationIfLess(key string, value time.Duration) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	if value < current {
		if err := df.SetDuration(value); err != nil {
			return 0, fmt.Errorf("failed to set duration value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetDurationIfEqual(key string, expected, newValue time.Duration) (time.Duration, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	if current == expected {
		if err := df.SetDuration(newValue); err != nil {
			return 0, fmt.Errorf("failed to set duration value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}
