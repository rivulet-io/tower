package op

import (
	"fmt"
	"time"
)

func (op *Operator) SetDuration(key string, value time.Duration) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetDuration(value); err != nil {
		return fmt.Errorf("failed to set duration value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetDuration(key string) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Duration()
	if err != nil {
		return 0, fmt.Errorf("failed to get duration value for key %s: %w", key, err)
	}

	return value, nil
}

func (op *Operator) AddDuration(key string, delta time.Duration) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SubDuration(key string, delta time.Duration) (time.Duration, error) {
	return op.AddDuration(key, -delta)
}

func (op *Operator) MulDuration(key string, factor int64) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) DivDuration(key string, divisor int64) (time.Duration, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("division by zero")
	}

	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) NegDuration(key string) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) AbsDuration(key string) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SwapDuration(key string, newValue time.Duration) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return 0, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return current, nil
}

func (op *Operator) CompareDuration(key string, value time.Duration) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

func (op *Operator) SetDurationIfGreater(key string, value time.Duration) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetDurationIfLess(key string, value time.Duration) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetDurationIfEqual(key string, expected, newValue time.Duration) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return 0, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}
