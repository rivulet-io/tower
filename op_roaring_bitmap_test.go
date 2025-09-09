package tower

import (
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func TestRoaringBitmapOperations(t *testing.T) {
	tower, err := NewTower(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    NewSizeFromMegabytes(64),
		MemTableSize: NewSizeFromMegabytes(16),
		BytesPerSync: NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test SetRoaringBitmap and GetRoaringBitmap
	t.Run("set and get roaring bitmap", func(t *testing.T) {
		key := "test_bitmap"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		err := tower.SetRoaringBitmap(key, bitmap)
		if err != nil {
			t.Errorf("SetRoaringBitmap failed: %v", err)
		}

		result, err := tower.GetRoaringBitmap(key)
		if err != nil {
			t.Errorf("GetRoaringBitmap failed: %v", err)
		}

		if !bitmap.Equals(result) {
			t.Errorf("Bitmaps are not equal")
		}
	})

	// Test SetRoaringBitmapBytes and GetRoaringBitmapBytes
	t.Run("set and get roaring bitmap bytes", func(t *testing.T) {
		key := "test_bitmap_bytes"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		data, err := bitmap.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to marshal bitmap: %v", err)
		}

		err = tower.SetRoaringBitmapBytes(key, data)
		if err != nil {
			t.Errorf("SetRoaringBitmapBytes failed: %v", err)
		}

		result, err := tower.GetRoaringBitmapBytes(key)
		if err != nil {
			t.Errorf("GetRoaringBitmapBytes failed: %v", err)
		}

		if len(result) != len(data) {
			t.Errorf("Byte lengths don't match: expected %d, got %d", len(data), len(result))
		}
	})

	// Test AddBit
	t.Run("add bit", func(t *testing.T) {
		key := "add_bit_test"
		bitmap := roaring.New()
		bitmap.Add(1)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.AddBit(key, 5)
		if err != nil {
			t.Errorf("AddBit failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if !result.Contains(1) || !result.Contains(5) {
			t.Errorf("Bits not added correctly")
		}
	})

	// Test RemoveBit
	t.Run("remove bit", func(t *testing.T) {
		key := "remove_bit_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.RemoveBit(key, 5)
		if err != nil {
			t.Errorf("RemoveBit failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if !result.Contains(1) || result.Contains(5) {
			t.Errorf("Bit not removed correctly")
		}
	})

	// Test HasBit
	t.Run("has bit", func(t *testing.T) {
		key := "has_bit_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)

		tower.SetRoaringBitmap(key, bitmap)

		hasBit, err := tower.HasBit(key, 1)
		if err != nil {
			t.Errorf("HasBit failed: %v", err)
		}
		if !hasBit {
			t.Errorf("Expected bit 1 to be present")
		}

		hasBit, err = tower.HasBit(key, 10)
		if err != nil {
			t.Errorf("HasBit failed: %v", err)
		}
		if hasBit {
			t.Errorf("Expected bit 10 to be absent")
		}
	})

	// Test UnionRoaringBitmap
	t.Run("union roaring bitmap", func(t *testing.T) {
		key := "union_test"
		bitmap1 := roaring.New()
		bitmap1.Add(1)
		bitmap1.Add(2)

		bitmap2 := roaring.New()
		bitmap2.Add(2)
		bitmap2.Add(3)

		tower.SetRoaringBitmap(key, bitmap1)

		err := tower.UnionRoaringBitmap(key, bitmap2)
		if err != nil {
			t.Errorf("UnionRoaringBitmap failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if !result.Contains(1) || !result.Contains(2) || !result.Contains(3) {
			t.Errorf("Union operation failed")
		}
	})

	// Test IntersectRoaringBitmap
	t.Run("intersect roaring bitmap", func(t *testing.T) {
		key := "intersect_test"
		bitmap1 := roaring.New()
		bitmap1.Add(1)
		bitmap1.Add(2)
		bitmap1.Add(3)

		bitmap2 := roaring.New()
		bitmap2.Add(2)
		bitmap2.Add(3)
		bitmap2.Add(4)

		tower.SetRoaringBitmap(key, bitmap1)

		err := tower.IntersectRoaringBitmap(key, bitmap2)
		if err != nil {
			t.Errorf("IntersectRoaringBitmap failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if result.Contains(1) || !result.Contains(2) || !result.Contains(3) || result.Contains(4) {
			t.Errorf("Intersection operation failed")
		}
	})

	// Test DifferenceRoaringBitmap
	t.Run("difference roaring bitmap", func(t *testing.T) {
		key := "difference_test"
		bitmap1 := roaring.New()
		bitmap1.Add(1)
		bitmap1.Add(2)
		bitmap1.Add(3)

		bitmap2 := roaring.New()
		bitmap2.Add(2)
		bitmap2.Add(4)

		tower.SetRoaringBitmap(key, bitmap1)

		err := tower.DifferenceRoaringBitmap(key, bitmap2)
		if err != nil {
			t.Errorf("DifferenceRoaringBitmap failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if !result.Contains(1) || result.Contains(2) || !result.Contains(3) || result.Contains(4) {
			t.Errorf("Difference operation failed")
		}
	})

	// Test AndBits
	t.Run("and bits", func(t *testing.T) {
		key := "and_bits_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(2)
		bitmap.Add(3)
		bitmap.Add(4)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.AndBits(key, 2, 3, 5)
		if err != nil {
			t.Errorf("AndBits failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if result.Contains(1) || !result.Contains(2) || !result.Contains(3) || result.Contains(4) || result.Contains(5) {
			t.Errorf("AndBits operation failed")
		}
	})

	// Test OrBits
	t.Run("or bits", func(t *testing.T) {
		key := "or_bits_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(2)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.OrBits(key, 3, 4, 5)
		if err != nil {
			t.Errorf("OrBits failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if !result.Contains(1) || !result.Contains(2) || !result.Contains(3) || !result.Contains(4) || !result.Contains(5) {
			t.Errorf("OrBits operation failed")
		}
	})

	// Test XorBits
	t.Run("xor bits", func(t *testing.T) {
		key := "xor_bits_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(2)
		bitmap.Add(3)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.XorBits(key, 2, 4, 5)
		if err != nil {
			t.Errorf("XorBits failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		// 1 should remain (not toggled), 2 should be removed (toggled), 3 should remain (not toggled), 4 should be added (toggled), 5 should be added (toggled)
		if !result.Contains(1) || result.Contains(2) || !result.Contains(3) || !result.Contains(4) || !result.Contains(5) {
			t.Errorf("XorBits operation failed")
		}
	})

	// Test CardinalityRoaringBitmap
	t.Run("cardinality roaring bitmap", func(t *testing.T) {
		key := "cardinality_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)
		bitmap.Add(15)

		tower.SetRoaringBitmap(key, bitmap)

		count, err := tower.CardinalityRoaringBitmap(key)
		if err != nil {
			t.Errorf("CardinalityRoaringBitmap failed: %v", err)
		}

		if count != 4 {
			t.Errorf("Expected cardinality 4, got %d", count)
		}
	})

	// Test ClearRoaringBitmap
	t.Run("clear roaring bitmap", func(t *testing.T) {
		key := "clear_test"
		bitmap := roaring.New()
		bitmap.Add(1)
		bitmap.Add(5)
		bitmap.Add(10)

		tower.SetRoaringBitmap(key, bitmap)

		err := tower.ClearRoaringBitmap(key)
		if err != nil {
			t.Errorf("ClearRoaringBitmap failed: %v", err)
		}

		result, _ := tower.GetRoaringBitmap(key)
		if result.GetCardinality() != 0 {
			t.Errorf("Bitmap not cleared correctly")
		}
	})
}
