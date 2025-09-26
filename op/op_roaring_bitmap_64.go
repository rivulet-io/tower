package op

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// RoaringBitmap64 operations
func (op *Operator) SetRoaringBitmap64(key string, value *roaring64.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetRoaringBitmap64(value); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetRoaringBitmap64(key string) (*roaring64.Bitmap, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.RoaringBitmap64()
	if err != nil {
		return nil, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return value, nil
}

func (op *Operator) SetRoaringBitmap64Bytes(key string, value []byte) error {
	bitmap := roaring64.New()
	if err := bitmap.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("failed to unmarshal roaring bitmap64 from bytes: %w", err)
	}

	return op.SetRoaringBitmap64(key, bitmap)
}

func (op *Operator) GetRoaringBitmap64Bytes(key string) ([]byte, error) {
	bitmap, err := op.GetRoaringBitmap64(key)
	if err != nil {
		return nil, err
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal roaring bitmap64 to bytes: %w", err)
	}

	return data, nil
}

// Basic bit operations (64-bit)
func (op *Operator) AddBitmap64Bit(key string, bit uint64) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	bitmap.Add(bit)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) DeleteBitmap64Bit(key string, bit uint64) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	bitmap.Remove(bit)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) ContainsBitmap64Bit(key string, bit uint64) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return false, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return bitmap.Contains(bit), nil
}

// Set operations (64-bit)
func (op *Operator) UnionBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	bitmap.Or(other)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) IntersectBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	bitmap.And(other)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) DifferenceBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	bitmap.AndNot(other)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) AndBits64(key string, bits ...uint64) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// Create new bitmap containing only given bits
	newBitmap := roaring64.New()
	for _, bit := range bits {
		newBitmap.Add(bit)
	}

	// Perform AND operation
	bitmap.And(newBitmap)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) OrBits64(key string, bits ...uint64) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// Add given bits
	for _, bit := range bits {
		bitmap.Add(bit)
	}

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) XorBits64(key string, bits ...uint64) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// Toggle given bits
	for _, bit := range bits {
		if bitmap.Contains(bit) {
			bitmap.Remove(bit)
		} else {
			bitmap.Add(bit)
		}
	}

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetBitmap64Cardinality(key string) (uint64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return 0, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return bitmap.GetCardinality(), nil
}

func (op *Operator) ClearRoaringBitmap64(key string) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	bitmap := roaring64.New()
	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}
