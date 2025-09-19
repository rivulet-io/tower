package op

import (
	"fmt"
	"time"
)

func (op *Operator) SetTimestamp(key string, value time.Time) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetTimestamp(value); err != nil {
		return fmt.Errorf("failed to set timestamp value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetTimestamp(key string) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	return value, nil
}

func (op *Operator) AddDurationToTimestamp(key string, duration time.Duration) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	newValue := current.Add(duration)
	if err := df.SetTimestamp(newValue); err != nil {
		return time.Time{}, fmt.Errorf("failed to set timestamp value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SubDurationFromTimestamp(key string, duration time.Duration) (time.Time, error) {
	return op.AddDurationToTimestamp(key, -duration)
}

func (op *Operator) CompareTimestamp(key string, value time.Time) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Timestamp()
	if err != nil {
		return 0, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	if current.Before(value) {
		return -1, nil
	} else if current.After(value) {
		return 1, nil
	}
	return 0, nil
}

func (op *Operator) SetTimestampIfGreater(key string, value time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	if value.After(current) {
		if err := df.SetTimestamp(value); err != nil {
			return time.Time{}, fmt.Errorf("failed to set timestamp value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetTimestampIfLess(key string, value time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	if value.Before(current) {
		if err := df.SetTimestamp(value); err != nil {
			return time.Time{}, fmt.Errorf("failed to set timestamp value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (op *Operator) SetTimestampIfEqual(key string, expected, newValue time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	if current.Equal(expected) {
		if err := df.SetTimestamp(newValue); err != nil {
			return time.Time{}, fmt.Errorf("failed to set timestamp value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}
