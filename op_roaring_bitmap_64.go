package tower

import (
	"fmt"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// RoaringBitmap64 연산들
func (t *Tower) SetRoaringBitmap64(key string, value *roaring64.Bitmap) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetRoaringBitmap64(value); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) GetRoaringBitmap64(key string) (*roaring64.Bitmap, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	value, err := df.RoaringBitmap64()
	if err != nil {
		return nil, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return value, nil
}

func (t *Tower) SetRoaringBitmap64Bytes(key string, value []byte) error {
	bitmap := roaring64.New()
	if err := bitmap.UnmarshalBinary(value); err != nil {
		return fmt.Errorf("failed to unmarshal roaring bitmap64 from bytes: %w", err)
	}

	return t.SetRoaringBitmap64(key, bitmap)
}

func (t *Tower) GetRoaringBitmap64Bytes(key string) ([]byte, error) {
	bitmap, err := t.GetRoaringBitmap64(key)
	if err != nil {
		return nil, err
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal roaring bitmap64 to bytes: %w", err)
	}

	return data, nil
}

// 기본 비트 연산 (64비트)
func (t *Tower) AddBit64(key string, bit uint64) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) RemoveBit64(key string, bit uint64) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) HasBit64(key string, bit uint64) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return false, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return bitmap.Contains(bit), nil
}

// 집합 연산 (64비트)
func (t *Tower) UnionRoaringBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) IntersectRoaringBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) DifferenceRoaringBitmap64(key string, other *roaring64.Bitmap) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// 가변 파라미터를 사용한 비트 연산 (64비트)
func (t *Tower) AndBits64(key string, bits ...uint64) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// 새로운 비트맵 생성하여 주어진 비트들만 포함
	newBitmap := roaring64.New()
	for _, bit := range bits {
		newBitmap.Add(bit)
	}

	// AND 연산 수행
	bitmap.And(newBitmap)

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) OrBits64(key string, bits ...uint64) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// 주어진 비트들 추가
	for _, bit := range bits {
		bitmap.Add(bit)
	}

	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) XorBits64(key string, bits ...uint64) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	// 주어진 비트들 토글
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

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// 추가 유틸리티 함수 (64비트)
func (t *Tower) CardinalityRoaringBitmap64(key string) (uint64, error) {
	unlock := t.rlock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	bitmap, err := df.RoaringBitmap64()
	if err != nil {
		return 0, fmt.Errorf("failed to get roaring bitmap64 value for key %s: %w", key, err)
	}

	return bitmap.GetCardinality(), nil
}

func (t *Tower) ClearRoaringBitmap64(key string) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	bitmap := roaring64.New()
	if err := df.SetRoaringBitmap64(bitmap); err != nil {
		return fmt.Errorf("failed to set roaring bitmap64 value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}
