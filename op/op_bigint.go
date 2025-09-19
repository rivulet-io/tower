package op

import (
	"fmt"
	"math/big"
)

// ================================
// BigInt Operations
// ================================

// SetBigInt sets a BigInt value for the given key
func (op *Operator) SetBigInt(key string, value *big.Int) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	err := df.SetBigInt(value)
	if err != nil {
		return fmt.Errorf("failed to set BigInt: %w", err)
	}

	return op.set(key, df)
}

// GetBigInt retrieves a BigInt value for the given key
func (op *Operator) GetBigInt(key string) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	return df.BigInt()
}

// AddBigInt adds a value to the BigInt stored at key
func (op *Operator) AddBigInt(key string, delta *big.Int) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Add(current, delta)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// SubBigInt subtracts a value from the BigInt stored at key
func (op *Operator) SubBigInt(key string, delta *big.Int) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Sub(current, delta)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// MulBigInt multiplies the BigInt stored at key by a factor
func (op *Operator) MulBigInt(key string, factor *big.Int) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Mul(current, factor)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// DivBigInt divides the BigInt stored at key by a divisor
func (op *Operator) DivBigInt(key string, divisor *big.Int) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	if divisor.Sign() == 0 {
		return nil, fmt.Errorf("division by zero")
	}

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Div(current, divisor)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// ModBigInt computes the modulus of the BigInt stored at key
func (op *Operator) ModBigInt(key string, modulus *big.Int) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	if modulus.Sign() == 0 {
		return nil, fmt.Errorf("modulo by zero")
	}

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Mod(current, modulus)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// CmpBigInt compares the BigInt stored at key with another value
func (op *Operator) CmpBigInt(key string, other *big.Int) (int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, err
	}

	if df.Type() != TypeBigInt {
		return 0, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return 0, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	return current.Cmp(other), nil
}

// NegBigInt negates the BigInt stored at key
func (op *Operator) NegBigInt(key string) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Neg(current)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}

// AbsBigInt computes the absolute value of the BigInt stored at key
func (op *Operator) AbsBigInt(key string) (*big.Int, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, err
	}

	if df.Type() != TypeBigInt {
		return nil, fmt.Errorf("key %s is not a BigInt", key)
	}

	current, err := df.BigInt()
	if err != nil {
		return nil, fmt.Errorf("failed to get current BigInt: %w", err)
	}

	result := new(big.Int).Abs(current)

	err = df.SetBigInt(result)
	if err != nil {
		return nil, fmt.Errorf("failed to set result BigInt: %w", err)
	}

	err = op.set(key, df)
	if err != nil {
		return nil, fmt.Errorf("failed to store result: %w", err)
	}

	return result, nil
}
