package op

import (
	"fmt"
)

func (op *Operator) SetBool(key string, value bool) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetBool(value); err != nil {
		return fmt.Errorf("failed to set bool value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetBool(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	return value, nil
}

// Logical operations
func (op *Operator) AndBool(key string, other bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) OrBool(key string, other bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) XorBool(key string, other bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

func (op *Operator) NotBool(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// Comparison operations
func (op *Operator) EqualBool(key string, other bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	current, err := df.Bool()
	if err != nil {
		return false, fmt.Errorf("failed to get bool value for key %s: %w", key, err)
	}

	return current == other, nil
}

// Toggle operations
func (op *Operator) ToggleBool(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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

	if err := op.set(key, df); err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return newValue, nil
}

// Conditional set operations
func (op *Operator) SetBoolIfTrue(key string, condition bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return true, nil
	}

	return current, nil
}

func (op *Operator) SetBoolIfFalse(key string, condition bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return false, nil
	}

	return current, nil
}

func (op *Operator) SetBoolIfEqual(key string, expected, newValue bool) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
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
		if err := op.set(key, df); err != nil {
			return false, fmt.Errorf("failed to set key %s: %w", key, err)
		}
		return newValue, nil
	}

	return current, nil
}


