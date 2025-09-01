package tower

import (
	"testing"
)

func TestBloomFilter(t *testing.T) {
	// 테스트용 Tower 생성
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_bloom_filter"
	slots := 0 // 기본 슬롯 수 사용 (3)

	// Bloom filter 생성
	err := tower.CreateBloomFilter(key, slots)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter: %v", err)
	}

	// 요소 추가
	items := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for _, item := range items {
		err := tower.BloomFilterAdd(key, item)
		if err != nil {
			t.Fatalf("Failed to add item %s: %v", item, err)
		}
	}

	// 추가된 요소 확인
	for _, item := range items {
		contains, err := tower.BloomFilterContains(key, item)
		if err != nil {
			t.Fatalf("Failed to check item %s: %v", item, err)
		}
		if !contains {
			t.Errorf("Item %s should be in the filter", item)
		}
	}

	// 없는 요소 확인 (거짓 양성 가능하지만, 이 테스트에서는 없다고 가정)
	nonExistent := []string{"fig", "grape", "honeydew"}
	for _, item := range nonExistent {
		contains, err := tower.BloomFilterContains(key, item)
		if err != nil {
			t.Fatalf("Failed to check non-existent item %s: %v", item, err)
		}
		// 거짓 양성이 발생할 수 있으므로, true라도 에러 아님
		t.Logf("Non-existent item %s: contains=%v", item, contains)
	}

	// Count 확인
	count, err := tower.BloomFilterCount(key)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	if count != uint64(len(items)) {
		t.Errorf("Expected count %d, got %d", len(items), count)
	}

	// 클리어 테스트
	err = tower.BloomFilterClear(key)
	if err != nil {
		t.Fatalf("Failed to clear Bloom filter: %v", err)
	}

	// 클리어 후 확인
	for _, item := range items {
		contains, err := tower.BloomFilterContains(key, item)
		if err != nil {
			t.Fatalf("Failed to check item after clear %s: %v", item, err)
		}
		if contains {
			t.Errorf("Item %s should not be in the filter after clear", item)
		}
	}

	// 클리어 후 Count 확인
	count, err = tower.BloomFilterCount(key)
	if err != nil {
		t.Fatalf("Failed to get count after clear: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0 after clear, got %d", count)
	}

	// 삭제 테스트
	err = tower.DeleteBloomFilter(key)
	if err != nil {
		t.Fatalf("Failed to delete Bloom filter: %v", err)
	}

	// 삭제 후 확인
	_, err = tower.BloomFilterContains(key, "apple")
	if err == nil {
		t.Error("Expected error after deleting Bloom filter")
	}
}

func TestBloomFilterSlots(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_slots"

	// 슬롯 수 3
	err := tower.CreateBloomFilter(key+"_3", 3)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter with 3 slots: %v", err)
	}

	// 슬롯 수 5
	err = tower.CreateBloomFilter(key+"_5", 5)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter with 5 slots: %v", err)
	}

	// 슬롯 수 6 (오류)
	err = tower.CreateBloomFilter(key+"_6", 6)
	if err == nil {
		t.Error("Expected error for slots > 5")
	}

	item := "test_item"

	err = tower.BloomFilterAdd(key+"_3", item)
	if err != nil {
		t.Fatalf("Failed to add to 3-slot filter: %v", err)
	}

	err = tower.BloomFilterAdd(key+"_5", item)
	if err != nil {
		t.Fatalf("Failed to add to 5-slot filter: %v", err)
	}

	contains3, _ := tower.BloomFilterContains(key+"_3", item)
	contains5, _ := tower.BloomFilterContains(key+"_5", item)

	if !contains3 {
		t.Error("Item should be in 3-slot filter")
	}
	if !contains5 {
		t.Error("Item should be in 5-slot filter")
	}
}
