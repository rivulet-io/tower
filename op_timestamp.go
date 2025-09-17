package tower

import (
	"fmt"
	"time"
)

func (t *Tower) SetTimestamp(key string, value time.Time) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetTimestamp(value); err != nil {
		return fmt.Errorf("failed to set timestamp value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetTimestamp(key string) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Timestamp()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get timestamp value for key %s: %w", key, err)
	}

	return value, nil
}

func (t *Tower) AddDurationToTimestamp(key string, duration time.Duration) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SubDurationFromTimestamp(key string, duration time.Duration) (time.Time, error) {
	return t.AddDurationToTimestamp(key, -duration)
}

func (t *Tower) CompareTimestamp(key string, value time.Time) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

func (t *Tower) SetTimestampIfGreater(key string, value time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetTimestampIfLess(key string, value time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}
	return current, nil
}

func (t *Tower) SetTimestampIfEqual(key string, expected, newValue time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}
	return current, nil
}
