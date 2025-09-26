package op

import (
	"fmt"
	"time"
)

func (op *Operator) SetTime(key string, value time.Time) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetTime(value); err != nil {
		return fmt.Errorf("failed to set time value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetTime(key string) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return value, nil
}

// Time calculation operations
func (op *Operator) AddTimeWithDuration(key string, duration time.Duration) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	newValue := current.Add(duration)
	if err := df.SetTime(newValue); err != nil {
		return time.Time{}, fmt.Errorf("failed to set time value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) SubTimeWithDuration(key string, duration time.Duration) (time.Time, error) {
	return op.AddTimeWithDuration(key, -duration)
}

// Comparison operations
func (op *Operator) CompareTimeBefore(key string, other time.Time) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Before(other), nil
}

func (op *Operator) CompareTimeAfter(key string, other time.Time) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.After(other), nil
}

func (op *Operator) CompareTimeEqual(key string, other time.Time) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Equal(other), nil
}

func (op *Operator) CalculateTimeDiff(key string, other time.Time) (time.Duration, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Sub(other), nil
}

// Utility operations
func (op *Operator) CheckTimeZero(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.IsZero(), nil
}

func (op *Operator) SetTimeIfGreater(key string, value time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	if value.After(current) {
		if err := df.SetTime(value); err != nil {
			return time.Time{}, fmt.Errorf("failed to set time value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}

	return current, nil
}

func (op *Operator) SetTimeIfLess(key string, value time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	if value.Before(current) {
		if err := df.SetTime(value); err != nil {
			return time.Time{}, fmt.Errorf("failed to set time value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}

	return current, nil
}

func (op *Operator) SetTimeIfEqual(key string, expected, newValue time.Time) (time.Time, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	if current.Equal(expected) {
		if err := df.SetTime(newValue); err != nil {
			return time.Time{}, fmt.Errorf("failed to set time value: %w", err)
		}
		if err := op.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}

// Time element extraction
func (op *Operator) GetTimeYear(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Year(), nil
}

func (op *Operator) GetTimeMonth(key string) (time.Month, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Month(), nil
}

func (op *Operator) GetTimeDay(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Day(), nil
}

func (op *Operator) GetTimeHour(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Hour(), nil
}

func (op *Operator) GetTimeMinute(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Minute(), nil
}

func (op *Operator) GetTimeSecond(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Second(), nil
}

func (op *Operator) GetTimeNanosecond(key string) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Nanosecond(), nil
}

