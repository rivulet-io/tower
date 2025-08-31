package tower

import (
	"fmt"
	"math"
)

// ================================
// Decimal Operations (Fixed-Point Arithmetic)
// ================================

// SetDecimal sets a decimal value for the given key
func (tw *Tower) SetDecimal(key string, coefficient int64, scale int32) error {
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
func (tw *Tower) GetDecimal(key string) (coefficient int64, scale int32, err error) {
	unlock := tw.rlock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return 0, 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, 0, fmt.Errorf("key %s is not a decimal", key)
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

	if scale > 18 {
		return fmt.Errorf("scale too large: maximum supported scale is 18")
	}

	// Convert float to fixed-point representation
	multiplier := math.Pow10(int(scale))
	coefficient := int64(value * multiplier)

	df := NULLDataFrame()
	err := df.SetDecimal(coefficient, scale)
	if err != nil {
		return fmt.Errorf("failed to set decimal: %w", err)
	}

	return tw.set(key, df)
}

// GetDecimalAsFloat retrieves a decimal value as float64
func (tw *Tower) GetDecimalAsFloat(key string) (float64, error) {
	unlock := tw.rlock(key)
	defer unlock()

	coefficient, scale, err := tw.GetDecimal(key)
	if err != nil {
		return 0, err
	}

	if scale > 18 {
		return 0, fmt.Errorf("scale too large: maximum supported scale is 18")
	}

	divisor := math.Pow10(int(scale))
	return float64(coefficient) / divisor, nil
}

// AddDecimal adds a decimal value to the decimal stored at key
func (tw *Tower) AddDecimal(key string, deltaCoefficient int64, deltaScale int32) (int64, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return 0, 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Align scales
	resultCoeff, resultScale := addDecimals(currentCoeff, currentScale, deltaCoefficient, deltaScale)

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// SubDecimal subtracts a decimal value from the decimal stored at key
func (tw *Tower) SubDecimal(key string, deltaCoefficient int64, deltaScale int32) (int64, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return 0, 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Negate delta and add
	resultCoeff, resultScale := addDecimals(currentCoeff, currentScale, -deltaCoefficient, deltaScale)

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// MulDecimal multiplies the decimal stored at key by a factor
func (tw *Tower) MulDecimal(key string, factorCoefficient int64, factorScale int32) (int64, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	df, err := tw.get(key)
	if err != nil {
		return 0, 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Multiply coefficients and add scales
	resultCoeff := currentCoeff * factorCoefficient
	resultScale := currentScale + factorScale

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// DivDecimal divides the decimal stored at key by a divisor
func (tw *Tower) DivDecimal(key string, divisorCoefficient int64, divisorScale int32, resultScale int32) (int64, int32, error) {
	unlock := tw.lock(key)
	defer unlock()

	if divisorCoefficient == 0 {
		return 0, 0, fmt.Errorf("division by zero")
	}

	df, err := tw.get(key)
	if err != nil {
		return 0, 0, err
	}

	if df.Type() != TypeDecimal {
		return 0, 0, fmt.Errorf("key %s is not a decimal", key)
	}

	currentCoeff, currentScale, err := df.Decimal()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get current decimal: %w", err)
	}

	// Align scales for division
	// dividend = currentCoeff * 10^(resultScale - currentScale)
	// divisor = divisorCoefficient * 10^(resultScale - divisorScale)
	dividend := currentCoeff
	divisor := divisorCoefficient

	scaleDiff := resultScale - currentScale
	if scaleDiff > 0 {
		if scaleDiff > 18 {
			dividendFloat := float64(currentCoeff) * math.Pow10(int(scaleDiff))
			dividend = int64(dividendFloat)
		} else {
			dividend *= int64(math.Pow10(int(scaleDiff)))
		}
	} else if scaleDiff < 0 {
		if -scaleDiff > 18 {
			divisorFloat := float64(divisorCoefficient) * math.Pow10(int(-scaleDiff))
			divisor = int64(divisorFloat)
		} else {
			divisor *= int64(math.Pow10(int(-scaleDiff)))
		}
	}

	scaleDiff = resultScale - divisorScale
	if scaleDiff > 0 {
		if scaleDiff > 18 {
			divisorFloat := float64(divisor) * math.Pow10(int(scaleDiff))
			divisor = int64(divisorFloat)
		} else {
			divisor *= int64(math.Pow10(int(scaleDiff)))
		}
	} else if scaleDiff < 0 {
		if -scaleDiff > 18 {
			dividendFloat := float64(dividend) * math.Pow10(int(-scaleDiff))
			dividend = int64(dividendFloat)
		} else {
			dividend *= int64(math.Pow10(int(-scaleDiff)))
		}
	}

	resultCoeff := dividend / divisor

	err = df.SetDecimal(resultCoeff, resultScale)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to set result decimal: %w", err)
	}

	err = tw.set(key, df)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to store result: %w", err)
	}

	return resultCoeff, resultScale, nil
}

// CmpDecimal compares the decimal stored at key with another decimal
func (tw *Tower) CmpDecimal(key string, otherCoefficient int64, otherScale int32) (int, error) {
	unlock := tw.rlock(key)
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

// addDecimals adds two decimals with different scales
func addDecimals(coeff1 int64, scale1 int32, coeff2 int64, scale2 int32) (int64, int32) {
	if scale1 == scale2 {
		return coeff1 + coeff2, scale1
	}

	if scale1 > scale2 {
		// Scale up coeff2
		scaleDiff := scale1 - scale2
		if scaleDiff > 18 {
			// Handle large scale differences by using float64 intermediate
			factor := math.Pow10(int(scaleDiff))
			coeff2Float := float64(coeff2) * factor
			return coeff1 + int64(coeff2Float), scale1
		}
		coeff2 *= int64(math.Pow10(int(scaleDiff)))
		return coeff1 + coeff2, scale1
	} else {
		// Scale up coeff1
		scaleDiff := scale2 - scale1
		if scaleDiff > 18 {
			// Handle large scale differences by using float64 intermediate
			factor := math.Pow10(int(scaleDiff))
			coeff1Float := float64(coeff1) * factor
			return int64(coeff1Float) + coeff2, scale2
		}
		coeff1 *= int64(math.Pow10(int(scaleDiff)))
		return coeff1 + coeff2, scale2
	}
}

// compareDecimals compares two decimals
func compareDecimals(coeff1 int64, scale1 int32, coeff2 int64, scale2 int32) int {
	alignedCoeff1, alignedCoeff2, _ := alignDecimals(coeff1, scale1, coeff2, scale2)

	if alignedCoeff1 < alignedCoeff2 {
		return -1
	} else if alignedCoeff1 > alignedCoeff2 {
		return 1
	}
	return 0
}

// alignDecimals aligns two decimals to the same scale
func alignDecimals(coeff1 int64, scale1 int32, coeff2 int64, scale2 int32) (int64, int64, int32) {
	if scale1 == scale2 {
		return coeff1, coeff2, scale1
	}

	if scale1 > scale2 {
		scaleDiff := scale1 - scale2
		if scaleDiff > 18 {
			// Handle large scale differences by using float64 intermediate
			factor := math.Pow10(int(scaleDiff))
			coeff2Float := float64(coeff2) * factor
			return coeff1, int64(coeff2Float), scale1
		}
		coeff2 *= int64(math.Pow10(int(scaleDiff)))
		return coeff1, coeff2, scale1
	} else {
		scaleDiff := scale2 - scale1
		if scaleDiff > 18 {
			// Handle large scale differences by using float64 intermediate
			factor := math.Pow10(int(scaleDiff))
			coeff1Float := float64(coeff1) * factor
			return int64(coeff1Float), coeff2, scale2
		}
		coeff1 *= int64(math.Pow10(int(scaleDiff)))
		return coeff1, coeff2, scale2
	}
}
