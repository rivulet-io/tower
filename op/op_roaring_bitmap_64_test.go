package op

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/rivulet-io/tower/util/size"
)

func TestRoaringBitmap64Operations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test SetRoaringBitmap64 and GetRoaringBitmap64
	t.Run("set and get roaring bitmap64", func(t *testing.T) {
		key := "test_bitmap64"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		err := tower.SetRoaringBitmap64(key, bitmap)
		if err != nil {
			t.Errorf("SetRoaringBitmap64 failed: %v", err)
		}

		result, err := tower.GetRoaringBitmap64(key)
		if err != nil {
			t.Errorf("GetRoaringBitmap64 failed: %v", err)
		}

		if !bitmap.Equals(result) {
			t.Errorf("Bitmaps are not equal")
		}
	})

	// Test SetRoaringBitmap64Bytes and GetRoaringBitmap64Bytes
	t.Run("set and get roaring bitmap64 bytes", func(t *testing.T) {
		key := "test_bitmap64_bytes"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		data, err := bitmap.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to marshal bitmap: %v", err)
		}

		err = tower.SetRoaringBitmap64Bytes(key, data)
		if err != nil {
			t.Errorf("SetRoaringBitmap64Bytes failed: %v", err)
		}

		result, err := tower.GetRoaringBitmap64Bytes(key)
		if err != nil {
			t.Errorf("GetRoaringBitmap64Bytes failed: %v", err)
		}

		if len(result) != len(data) {
			t.Errorf("Byte lengths don't match: expected %d, got %d", len(data), len(result))
		}
	})

	// Test AddBit64
	t.Run("add bit64", func(t *testing.T) {
		key := "add_bit64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.AddBit64(key, 5)
		if err != nil {
			t.Errorf("AddBit64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if !result.Contains(1) || !result.Contains(5) {
			t.Errorf("Bits not added correctly")
		}
	})

	// Test RemoveBit64
	t.Run("remove bit64", func(t *testing.T) {
		key := "remove_bit64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.RemoveBit64(key, 5)
		if err != nil {
			t.Errorf("RemoveBit64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if !result.Contains(1) || result.Contains(5) {
			t.Errorf("Bit not removed correctly")
		}
	})

	// Test HasBit64
	t.Run("has bit64", func(t *testing.T) {
		key := "has_bit64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)

		tower.SetRoaringBitmap64(key, bitmap)

		hasBit, err := tower.HasBit64(key, 1)
		if err != nil {
			t.Errorf("HasBit64 failed: %v", err)
		}
		if !hasBit {
			t.Errorf("Expected bit 1 to be present")
		}

		hasBit, err = tower.HasBit64(key, 10)
		if err != nil {
			t.Errorf("HasBit64 failed: %v", err)
		}
		if hasBit {
			t.Errorf("Expected bit 10 to be absent")
		}
	})

	// Test UnionRoaringBitmap64
	t.Run("union roaring bitmap64", func(t *testing.T) {
		key := "union64_test"
		bitmap1 := roaring64.New()
		bitmap1.Add(1)
		bitmap1.Add(2)

		bitmap2 := roaring64.New()
		bitmap2.Add(2)
		bitmap2.Add(3)

		tower.SetRoaringBitmap64(key, bitmap1)

		err := tower.UnionRoaringBitmap64(key, bitmap2)
		if err != nil {
			t.Errorf("UnionRoaringBitmap64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if !result.Contains(1) || !result.Contains(2) || !result.Contains(3) {
			t.Errorf("Union operation failed")
		}
	})

	// Test IntersectRoaringBitmap64
	t.Run("intersect roaring bitmap64", func(t *testing.T) {
		key := "intersect64_test"
		bitmap1 := roaring64.New()
		bitmap1.Add(1)
		bitmap1.Add(2)
		bitmap1.Add(3)

		bitmap2 := roaring64.New()
		bitmap2.Add(2)
		bitmap2.Add(3)
		bitmap2.Add(4)

		tower.SetRoaringBitmap64(key, bitmap1)

		err := tower.IntersectRoaringBitmap64(key, bitmap2)
		if err != nil {
			t.Errorf("IntersectRoaringBitmap64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if result.Contains(1) || !result.Contains(2) || !result.Contains(3) || result.Contains(4) {
			t.Errorf("Intersection operation failed")
		}
	})

	// Test DifferenceRoaringBitmap64
	t.Run("difference roaring bitmap64", func(t *testing.T) {
		key := "difference64_test"
		bitmap1 := roaring64.New()
		bitmap1.Add(1)
		bitmap1.Add(2)
		bitmap1.Add(3)

		bitmap2 := roaring64.New()
		bitmap2.Add(2)
		bitmap2.Add(4)

		tower.SetRoaringBitmap64(key, bitmap1)

		err := tower.DifferenceRoaringBitmap64(key, bitmap2)
		if err != nil {
			t.Errorf("DifferenceRoaringBitmap64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if !result.Contains(1) || result.Contains(2) || !result.Contains(3) || result.Contains(4) {
			t.Errorf("Difference operation failed")
		}
	})

	// Test AndBits64
	t.Run("and bits64", func(t *testing.T) {
		key := "and_bits64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(2)
		bitmap.Add(3)
		bitmap.Add(4)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.AndBits64(key, 2, 3, 5)
		if err != nil {
			t.Errorf("AndBits64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if result.Contains(1) || !result.Contains(2) || !result.Contains(3) || result.Contains(4) || result.Contains(5) {
			t.Errorf("AndBits64 operation failed")
		}
	})

	// Test OrBits64
	t.Run("or bits64", func(t *testing.T) {
		key := "or_bits64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(2)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.OrBits64(key, 3, 4, 5)
		if err != nil {
			t.Errorf("OrBits64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if !result.Contains(1) || !result.Contains(2) || !result.Contains(3) || !result.Contains(4) || !result.Contains(5) {
			t.Errorf("OrBits64 operation failed")
		}
	})

	// Test XorBits64
	t.Run("xor bits64", func(t *testing.T) {
		key := "xor_bits64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(2)
		bitmap.Add(3)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.XorBits64(key, 2, 4, 5)
		if err != nil {
			t.Errorf("XorBits64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		// 1 should remain (not toggled), 2 should be removed (toggled), 3 should remain (not toggled), 4 should be added (toggled), 5 should be added (toggled)
		if !result.Contains(1) || result.Contains(2) || !result.Contains(3) || !result.Contains(4) || !result.Contains(5) {
			t.Errorf("XorBits64 operation failed")
		}
	})

	// Test CardinalityRoaringBitmap64
	t.Run("cardinality roaring bitmap64", func(t *testing.T) {
		key := "cardinality64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)
		bitmap.Add(15)

		tower.SetRoaringBitmap64(key, bitmap)

		count, err := tower.CardinalityRoaringBitmap64(key)
		if err != nil {
			t.Errorf("CardinalityRoaringBitmap64 failed: %v", err)
		}

		if count != 4 {
			t.Errorf("Expected cardinality 4, got %d", count)
		}
	})

	// Test ClearRoaringBitmap64
	t.Run("clear roaring bitmap64", func(t *testing.T) {
		key := "clear64_test"
		bitmap := roaring64.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		tower.SetRoaringBitmap64(key, bitmap)

		err := tower.ClearRoaringBitmap64(key)
		if err != nil {
			t.Errorf("ClearRoaringBitmap64 failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap64(key)
		if result.GetCardinality() != 0 {
			t.Errorf("Bitmap not cleared correctly")
		}
	})
}
