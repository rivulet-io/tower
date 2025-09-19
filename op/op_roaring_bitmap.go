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

// 기본 비트 연산
func (op *Operator) AddBit(key string, bit uint32) error {
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

func (op *Operator) RemoveBit(key string, bit uint32) error {
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

func (op *Operator) HasBit(key string, bit uint32) (bool, error) {
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

// 집합 연산
func (op *Operator) UnionRoaringBitmap(key string, other *roaring.Bitmap) error {
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

func (op *Operator) IntersectRoaringBitmap(key string, other *roaring.Bitmap) error {
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

func (op *Operator) DifferenceRoaringBitmap(key string, other *roaring.Bitmap) error {
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

// 가변 파라미터를 사용한 비트 연산
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

	// 새로운 비트맵 생성하여 주어진 비트들만 포함
	newBitmap := roaring.New()
	for _, bit := range bits {
		newBitmap.Add(bit)
	}

	// AND 연산 수행
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

	// 주어진 비트들 추가
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

	// 주어진 비트들 토글
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

// 추가 유틸리티 함수
func (op *Operator) CardinalityRoaringBitmap(key string) (uint64, error) {
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
