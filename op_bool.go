package tower

import "fmt"

func (t *Tower) SetBool(key string, value bool) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetBool(value); err != nil {
		return fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetBool(key string) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	return value, nil
}

// 논리 연산
func (t *Tower) AndBool(key string, other bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	newValue := current && other
	if err := df.SetBool(newValue); err != nil {
		return false, fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) OrBool(key string, other bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	newValue := current || other
	if err := df.SetBool(newValue); err != nil {
		return false, fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) XorBool(key string, other bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	newValue := current != other
	if err := df.SetBool(newValue); err != nil {
		return false, fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (t *Tower) NotBool(key string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	newValue := !current
	if err := df.SetBool(newValue); err != nil {
		return false, fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// 비교 연산
func (t *Tower) EqualBool(key string, other bool) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	return current == other, nil
}

// 토글 연산
func (t *Tower) ToggleBool(key string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	newValue := !current
	if err := df.SetBool(newValue); err != nil {
		return false, fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// 조건부 설정 연산
func (t *Tower) SetBoolIfTrue(key string, condition bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	if condition {
		if err := df.SetBool(true); err != nil {
			return false, fmt.Errorf("failed to set bool value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return true, nil
	}

	return current, nil
}

func (t *Tower) SetBoolIfFalse(key string, condition bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	if condition {
		if err := df.SetBool(false); err != nil {
			return false, fmt.Errorf("failed to set bool value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return false, nil
	}

	return current, nil
}

func (t *Tower) SetBoolIfEqual(key string, expected, newValue bool) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	if current == expected {
		if err := df.SetBool(newValue); err != nil {
			return false, fmt.Errorf("failed to set bool value: %w", err)
		}
		if err := t.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}
