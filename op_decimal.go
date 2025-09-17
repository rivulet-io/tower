package tower

import (
	"fmt"
	"math"
	"math/big"
)

// ================================
// Decimal Operations (Fixed-Point Arithmetic)
// ================================

// SetDecimal sets a decimal value for the given key
func (tw *Tower) SetDecimal(key string, coefficient *big.Int, scale int32) error {
	unlock := tw.lock(key)
	defer unlock()

	df := NULLDataFrame()
	err := df.SetDecimal(coefficient, scale)
	if err != nil {
		return fmt.Errorf("failed to set decimal: %w", err)
	}

	return tw.set(key, df)
}

// GetDecimal retrieves a decimal value for the given key
func (tw *Tower) GetDecimal(key string) (coefficient *big.Int, scale int32, err error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return nil, 0, err
	}

	if df.Type() != TypeDecimal {
		return nil, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	return df.Decimal()
}

// SetDecimalFromFloat sets a decimal value from a float64
func (tw *Tower) SetDecimalFromFloat(key string, value float64, scale int32) error {
	unlock := tw.lock(key)
	defer unlock()

	if scale < 0 {
		return fmt.Errorf("scale cannot be negative")
	}

	if scale > 15 {
		return fmt.Errorf("scale too large: maximum supported scale is 15 (limited by float64 precision)")
	}

	// Convert float to fixed-point representation using big.Float for precision
	f := new(big.Float).SetFloat64(value)
	multiplier := new(big.Float).SetFloat64(math.Pow10(int(scale)))
	f.Mul(f, multiplier)

	// Set rounding mode to Banker's Rounding (Round half to even)
	f.SetMode(big.ToNearestEven)
	coefficient, _ := f.Int(nil)

	df := NULLDataFrame()
	err := df.SetDecimal(coefficient, scale)
	if err != nil {
		return fmt.Errorf("failed to set decimal: %w", err)
	}

	return tw.set(key, df)
}

// GetDecimalAsFloat retrieves a decimal value as float64
func (tw *Tower) GetDecimalAsFloat(key string) (float64, error) {
	unlock := tw.lock(key)
	defer unlock()

	coefficient, scale, err := tw.GetDecimal(key)
	if err != nil {
		return 0, err
	}

	if scale > 15 {
		return 0, fmt.Errorf("scale too large: maximum supported scale is 15 (limited by float64 precision)")
	}

	// Convert big.Int to float64
	coeffFloat, _ := new(big.Float).SetInt(coefficient).Float64()
	divisor := math.Pow10(int(scale))
	return coeffFloat / divisor, nil
}

// AddDecimal adds a decimal value to the decimal stored at key
func (tw *Tower) AddDecimal(key string, deltaCoefficient *big.Int, deltaScale int32) (*big.Int, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return nil, 0, err
	}

	if df.Type() != TypeDecimal {
		return nil, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Align scales
	resultCoeff, resultScale := addDecimals(currentCoeff, currentScale, deltaCoefficient, deltaScale)

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// SubDecimal subtracts a decimal value from the decimal stored at key
func (tw *Tower) SubDecimal(key string, deltaCoefficient *big.Int, deltaScale int32) (*big.Int, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return nil, 0, err
	}

	if df.Type() != TypeDecimal {
		return nil, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Negate delta and add
	negDelta := new(big.Int).Neg(deltaCoefficient)
	resultCoeff, resultScale := addDecimals(currentCoeff, currentScale, negDelta, deltaScale)

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// MulDecimal multiplies the decimal stored at key by a factor
func (tw *Tower) MulDecimal(key string, factorCoefficient *big.Int, factorScale int32) (*big.Int, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return nil, 0, err
	}

	if df.Type() != TypeDecimal {
		return nil, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Multiply coefficients and add scales using math/big.Int
	resultCoeff := new(big.Int).Mul(currentCoeff, factorCoefficient)
	resultScale := currentScale + factorScale

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// DivDecimal divides the decimal stored at key by a divisor
func (tw *Tower) DivDecimal(key string, divisorCoefficient *big.Int, divisorScale int32, resultScale int32) (*big.Int, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	if divisorCoefficient.Sign() == 0 {
		return nil, 0, fmt.Errorf("division by zero")
	}

	df, err := tw.get(key)
	if err != nil {
		return nil, 0, err
	}

	if df.Type() != TypeDecimal {
		return nil, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Calculate the total scale adjustment needed for the dividend.
	// scaleFactor = resultScale + divisorScale - currentScale
	scaleFactor := int64(resultScale) + int64(divisorScale) - int64(currentScale)

	cDividend := new(big.Int).Set(currentCoeff)
	cDivisor := new(big.Int).Set(divisorCoefficient)

	if scaleFactor > 0 {
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(scaleFactor), nil)
		cDividend.Mul(cDividend, multiplier)
	} else if scaleFactor < 0 {
		// If scaleFactor is negative, we are effectively dividing the dividend.
		// This is equivalent to multiplying the divisor.
		divisorMultiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(-scaleFactor), nil)
		cDivisor.Mul(cDivisor, divisorMultiplier)
	}

	// Perform the division
	resultCoeff := new(big.Int).Div(cDividend, cDivisor)

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// CmpDecimal compares the decimal stored at key with another decimal
func (tw *Tower) CmpDecimal(key string, otherCoefficient *big.Int, otherScale int32) (int, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	return compareDecimals(currentCoeff, currentScale, otherCoefficient, otherScale), nil
}

// ================================
// Helper Functions for Decimal Operations
// ================================

// addDecimals adds two decimals with different scales using math/big.Int
func addDecimals(coeff1 *big.Int, scale1 int32, coeff2 *big.Int, scale2 int32) (*big.Int, int32) {
	c1, c2, finalScale := alignDecimals(coeff1, scale1, coeff2, scale2)
	result := new(big.Int).Add(c1, c2)
	return result, finalScale
}

// compareDecimals compares two decimals
func compareDecimals(coeff1 *big.Int, scale1 int32, coeff2 *big.Int, scale2 int32) int {
	alignedCoeff1, alignedCoeff2, _ := alignDecimals(coeff1, scale1, coeff2, scale2)

	if alignedCoeff1.Cmp(alignedCoeff2) < 0 {
		return -1
	} else if alignedCoeff1.Cmp(alignedCoeff2) > 0 {
		return 1
	}
	return 0
}

// alignDecimals aligns two decimals to the same scale using math/big.Int
func alignDecimals(coeff1 *big.Int, scale1 int32, coeff2 *big.Int, scale2 int32) (*big.Int, *big.Int, int32) {
	if scale1 == scale2 {
		return new(big.Int).Set(coeff1), new(big.Int).Set(coeff2), scale1
	}

	c1 := new(big.Int).Set(coeff1)
	c2 := new(big.Int).Set(coeff2)
	var finalScale int32

	if scale1 > scale2 {
		finalScale = scale1
		scaleDiff := scale1 - scale2
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		c2.Mul(c2, multiplier)
		return c1, c2, finalScale
	} else {
		finalScale = scale2
		scaleDiff := scale2 - scale1
		multiplier := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleDiff)), nil)
		c1.Mul(c1, multiplier)
		return c1, c2, finalScale
	}
}
