package op

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
)

func (op *Operator) SetRoaringBitmap(key string, value *roaring.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetRoaringBitmap(value); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) GetRoaringBitmap(key string) (*roaring.Bitmap, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.RoaringBitmap()
	if err != nil {
		return nil, fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	return value, nil
}

func (op *Operator) SetRoaringBitmapBytes(key string, value []byte) error {
	bitmap := roaring.New()
	if err := bitmap.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("failed to unmarshal roaring bitmap from bytes: %w", err)
	}

	return op.SetRoaringBitmap(key, bitmap)
}

func (op *Operator) GetRoaringBitmapBytes(key string) ([]byte, error) {
	bitmap, err := op.GetRoaringBitmap(key)
	if err != nil {
		return nil, err
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal roaring bitmap to bytes: %w", err)
	}

	return data, nil
}

// Basic bit operations
func (op *Operator) AddBitmapBit(key string, bit uint32) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	bitmap.Add(bit)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) DeleteBitmapBit(key string, bit uint32) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	bitmap.Remove(bit)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) ContainsBitmapBit(key string, bit uint32) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return false, fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	return bitmap.Contains(bit), nil
}

// Set operations
func (op *Operator) UnionBitmap(key string, other *roaring.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	bitmap.Or(other)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) IntersectBitmap(key string, other *roaring.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	bitmap.And(other)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) DifferenceBitmap(key string, other *roaring.Bitmap) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	bitmap.AndNot(other)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// Bit operations using variable parameters
func (op *Operator) AndBits(key string, bits ...uint32) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	// Create new bitmap containing only given bits
	newBitmap := roaring.New()
	for _, bit := range bits {
		newBitmap.Add(bit)
	}

	// Perform AND operation
	bitmap.And(newBitmap)

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) OrBits(key string, bits ...uint32) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	// Add given bits
	for _, bit := range bits {
		bitmap.Add(bit)
	}

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) XorBits(key string, bits ...uint32) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	// Toggle given bits
	for _, bit := range bits {
		if bitmap.Contains(bit) {
			bitmap.Remove(bit)
		} else {
			bitmap.Add(bit)
		}
	}

	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// Additional utility functions
func (op *Operator) GetBitmapCardinality(key string) (uint64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap()
	if err != nil {
		return 0, fmt.Errorf("failed to get roaring bitmap value for key %s: %w", key, err)
	}

	return bitmap.GetCardinality(), nil
}

func (op *Operator) ClearRoaringBitmap(key string) error {
	unlock := op.lock(key)
	defer unlock()

	df := NULLDataFrame()
	bitmap := roaring.New()
	if err := df.SetRoaringBitmap(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap value: %w", err)
	}

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}
