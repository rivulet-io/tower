package op

import (
	"testing"
)

func TestBloomFilter(t *testing.T) {
	// 테스트용 Operator 생성
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_bloom_filter"
	slots := 0 // 기본 슬롯 수 사용 (3)

	// Bloom filter 생성
	err := tower.CreateBloomFilter(key, slots)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter: %v", err)
	}

	// 추�?
	items := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for _, item := range items {
		err := tower.AddBloomFilter(key, item)
		if err != nil {
			t.Fatalf("Failed to add item %s: %v", item, err)
		}
	}

	// 추�???
	for _, item := range items {
		contains, err := tower.ContainsBloomFilter(key, item)
		if err != nil {
			t.Fatalf("Failed to check item %s: %v", item, err)
		}
		if !contains {
			t.Errorf("Item %s should be in the filter", item)
		}
	}

	// (거짓  가 ??
	nonExistent := []string{"fig", "grape", "honeydew"}
	for _, item := range nonExistent {
		contains, err := tower.ContainsBloomFilter(key, item)
		if err != nil {
			t.Fatalf("Failed to check non-existent item %s: %v", item, err)
		}
		// 거짓  true
		t.Logf("Non-existent item %s: contains=%v", item, contains)
	}

	// Count
	count, err := tower.CountBloomFilter(key)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}
	if count != uint64(len(items)) {
		t.Errorf("Expected count %d, got %d", len(items), count)
	}

	//
	err = tower.ClearBloomFilter(key)
	if err != nil {
		t.Fatalf("Failed to clear Bloom filter: %v", err)
	}

	//
	for _, item := range items {
		contains, err := tower.ContainsBloomFilter(key, item)
		if err != nil {
			t.Fatalf("Failed to check item after clear %s: %v", item, err)
		}
		if contains {
			t.Errorf("Item %s should not be in the filter after clear", item)
		}
	}

	//
	count, err = tower.CountBloomFilter(key)
	if err != nil {
		t.Fatalf("Failed to get count after clear: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0 after clear, got %d", count)
	}

	// ?
	err = tower.DeleteBloomFilter(key)
	if err != nil {
		t.Fatalf("Failed to delete Bloom filter: %v", err)
	}

	// ? ??
	_, err = tower.ContainsBloomFilter(key, "apple")
	if err == nil {
		t.Error("Expected error after deleting Bloom filter")
	}
}

func TestBloomFilterSlots(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_slots"

	// ??3
	err := tower.CreateBloomFilter(key+"_3", 3)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter with 3 slots: %v", err)
	}

	// ??5
	err = tower.CreateBloomFilter(key+"_5", 5)
	if err != nil {
		t.Fatalf("Failed to create Bloom filter with 5 slots: %v", err)
	}

	// ??6 (
	err = tower.CreateBloomFilter(key+"_6", 6)
	if err == nil {
		t.Error("Expected error for slots > 5")
	}

	item := "test_item"

	err = tower.AddBloomFilter(key+"_3", item)
	if err != nil {
		t.Fatalf("Failed to add to 3-slot filter: %v", err)
	}

	err = tower.AddBloomFilter(key+"_5", item)
	if err != nil {
		t.Fatalf("Failed to add to 5-slot filter: %v", err)
	}

	contains3, _ := tower.ContainsBloomFilter(key+"_3", item)
	contains5, _ := tower.ContainsBloomFilter(key+"_5", item)

	if !contains3 {
		t.Error("Item should be in 3-slot filter")
	}
	if !contains5 {
		t.Error("Item should be in 5-slot filter")
	}
}
