package op

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// CreateBloomFilter는 새로운 Bloom filter를 생성
func (op *Operator) CreateBloomFilter(key string, slots int) error {
	if slots == 0 {
		slots = 3 // 기본 슬롯 수
	}
	if slots < 3 || slots > 5 {
		return fmt.Errorf("slots must be between 3 and 5")
	}

	unlock := op.lock(key)
	defer unlock()

	// 이미 존재하는지 확인
	_, err := op.get(key)
	if err == nil {
		return fmt.Errorf("bloom filter %s already exists", key)
	}

	// BloomFilterData 생성
	data := &BloomFilterData{
		Prefix: key,
		Slots:  slots,
		Salt:   "bloom_salt_2025",
		Count:  0,
	}

	df := NULLDataFrame()
	err = df.SetBloomFilter(data)
	if err != nil {
		return fmt.Errorf("failed to set bloom filter data: %w", err)
	}

	return op.set(key, df)
}

// BloomFilterAdd는 Bloom filter에 요소를 추가
func (op *Operator) BloomFilterAdd(key, item string) error {
	unlock := op.lock(key)
	defer unlock()

	// 메타데이터 가져오기
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// 해시 슬롯 계산
	slots := op.getBloomFilterSlots(item, bfd.Slots, bfd.Salt)

	// 슬롯 값을 바이트로 변환
	slotBytes := make([]byte, bfd.Slots*4)
	for i, slot := range slots {
		binary.BigEndian.PutUint32(slotBytes[i*4:], uint32(slot))
	}

	// 항목 저장
	itemKey := string(MakeBloomFilterItemKey(bfd.Prefix, item))
	itemDf := NULLDataFrame()
	err = itemDf.SetBinary(slotBytes)
	if err != nil {
		return fmt.Errorf("failed to set slot data: %w", err)
	}

	err = op.set(itemKey, itemDf)
	if err != nil {
		return fmt.Errorf("failed to set item: %w", err)
	}

	// Count 업데이트
	bfd.Count++
	err = df.SetBloomFilter(bfd)
	if err != nil {
		return fmt.Errorf("failed to update bloom filter data: %w", err)
	}

	return op.set(key, df)
}

// BloomFilterContains는 요소가 Bloom filter에 있는지 확인
func (op *Operator) BloomFilterContains(key, item string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	// 메타데이터 가져오기
	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return false, fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// 해시 슬롯 계산
	slots := op.getBloomFilterSlots(item, bfd.Slots, bfd.Salt)

	// 항목 가져오기
	itemKey := string(MakeBloomFilterItemKey(bfd.Prefix, item))
	itemDf, err := op.get(itemKey)
	if err != nil {
		// 키가 없으면 포함되지 않음
		return false, nil
	}

	slotBytes, err := itemDf.Binary()
	if err != nil {
		return false, fmt.Errorf("failed to get slot bytes: %w", err)
	}

	if len(slotBytes) != bfd.Slots*4 {
		return false, fmt.Errorf("invalid slot data length")
	}

	// 슬롯 비교
	for i := 0; i < bfd.Slots; i++ {
		storedSlot := int(binary.BigEndian.Uint32(slotBytes[i*4:]))
		if storedSlot != slots[i] {
			return false, nil
		}
	}

	return true, nil
}

// BloomFilterClear는 Bloom filter를 초기화
func (op *Operator) BloomFilterClear(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// 메타데이터 가져오기
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// 모든 항목 삭제
	prefix := string(MakeBloomFilterEntryKey(bfd.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		return op.delete(k)
	})
	if err != nil {
		return fmt.Errorf("failed to clear items: %w", err)
	}

	// Count 리셋
	bfd.Count = 0
	err = df.SetBloomFilter(bfd)
	if err != nil {
		return fmt.Errorf("failed to update bloom filter data: %w", err)
	}

	return op.set(key, df)
}

// BloomFilterCount는 Bloom filter의 요소 수를 반환
func (op *Operator) BloomFilterCount(key string) (uint64, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return 0, fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return 0, fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	return bfd.Count, nil
}

// DeleteBloomFilter는 Bloom filter를 완전히 삭제
func (op *Operator) DeleteBloomFilter(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteBloomFilter(key)
}

func (op *Operator) deleteBloomFilter(key string) error {
	// 메타데이터 가져오기
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// 모든 항목 삭제
	prefix := string(MakeBloomFilterEntryKey(bfd.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		return op.delete(k)
	})
	if err != nil {
		return fmt.Errorf("failed to delete items: %w", err)
	}

	// 메타데이터 삭제
	return op.delete(key)
}

// getBloomFilterSlots는 요소에 대한 해시 슬롯을 계산
func (op *Operator) getBloomFilterSlots(item string, slots int, salt string) []int {
	h := fnv.New64a()
	h.Write([]byte(item + salt))
	baseHash := h.Sum64()

	result := make([]int, slots)
	for i := 0; i < slots; i++ {
		hash := baseHash + uint64(i)*0x9e3779b97f4a7c15
		result[i] = int(hash % 1000000)
	}

	return result
}
