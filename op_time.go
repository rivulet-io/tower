package tower

import (
	"fmt"
	"time"
)

func (t *Tower) SetTime(key string, value time.Time) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetTime(value); err != nil {
		return fmt.Errorf("failed to set time value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetTime(key string) (time.Time, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Time()
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return value, nil
}

// 시간 계산 연산
func (t *Tower) AddDurationToTime(key string, duration time.Duration) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) SubDurationFromTime(key string, duration time.Duration) (time.Time, error) {
	return t.AddDurationToTime(key, -duration)
}

// 비교 연산
func (t *Tower) TimeBefore(key string, other time.Time) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Before(other), nil
}

func (t *Tower) TimeAfter(key string, other time.Time) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.After(other), nil
}

func (t *Tower) TimeEqual(key string, other time.Time) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Equal(other), nil
}

func (t *Tower) TimeDiff(key string, other time.Time) (time.Duration, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Sub(other), nil
}

// 유틸리티 연산
func (t *Tower) IsZeroTime(key string) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return false, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.IsZero(), nil
}

func (t *Tower) SetTimeIfGreater(key string, value time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}

	return current, nil
}

func (t *Tower) SetTimeIfLess(key string, value time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return value, nil
	}

	return current, nil
}

func (t *Tower) SetTimeIfEqual(key string, expected, newValue time.Time) (time.Time, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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
		if err := t.set(key, df); err != nil {
			return time.Time{}, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}

// 시간 요소 추출
func (t *Tower) GetTimeYear(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Year(), nil
}

func (t *Tower) GetTimeMonth(key string) (time.Month, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Month(), nil
}

func (t *Tower) GetTimeDay(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Day(), nil
}

func (t *Tower) GetTimeHour(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Hour(), nil
}

func (t *Tower) GetTimeMinute(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Minute(), nil
}

func (t *Tower) GetTimeSecond(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Second(), nil
}

func (t *Tower) GetTimeNanosecond(key string) (int, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Time()
	if err != nil {
		return 0, fmt.Errorf("failed to get time value for key %s: %w", key, err)
	}

	return current.Nanosecond(), nil
}
