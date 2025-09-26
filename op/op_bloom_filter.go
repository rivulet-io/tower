package op

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// CreateBloomFilter creates a new Bloom filter
func (op *Operator) CreateBloomFilter(key string, slots int) error {
	if slots == 0 {
		slots = 3 // Default slot count
	}
	if slots < 3 || slots > 5 {
		return fmt.Errorf("slots must be between 3 and 5")
	}

	unlock := op.lock(key)
	defer unlock()

	// Check if already exists
	_, err := op.get(key)
	if err == nil {
		return fmt.Errorf("bloom filter %s already exists", key)
	}

	// Create BloomFilterData
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

// AddBloomFilter adds an element to the Bloom filter
func (op *Operator) AddBloomFilter(key, item string) error {
	unlock := op.lock(key)
	defer unlock()

	// Get metadata
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// Calculate hash slot
	slots := op.getBloomFilterSlots(item, bfd.Slots, bfd.Salt)

	// Convert slot value to bytes
	slotBytes := make([]byte, bfd.Slots*4)
	for i, slot := range slots {
		binary.BigEndian.PutUint32(slotBytes[i*4:], uint32(slot))
	}

	// Store item
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

	// Update Count
	bfd.Count++
	err = df.SetBloomFilter(bfd)
	if err != nil {
		return fmt.Errorf("failed to update bloom filter data: %w", err)
	}

	return op.set(key, df)
}

// ContainsBloomFilter checks if element exists in Bloom filter
func (op *Operator) ContainsBloomFilter(key, item string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	// Get metadata
	df, err := op.get(key)
	if err != nil {
		return false, fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return false, fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// Calculate hash slot
	slots := op.getBloomFilterSlots(item, bfd.Slots, bfd.Salt)

	// Get item
	itemKey := string(MakeBloomFilterItemKey(bfd.Prefix, item))
	itemDf, err := op.get(itemKey)
	if err != nil {
		// Not included if key does not exist
		return false, nil
	}

	slotBytes, err := itemDf.Binary()
	if err != nil {
		return false, fmt.Errorf("failed to get slot bytes: %w", err)
	}

	if len(slotBytes) != bfd.Slots*4 {
		return false, fmt.Errorf("invalid slot data length")
	}

	// Compare slots
	for i := 0; i < bfd.Slots; i++ {
		storedSlot := int(binary.BigEndian.Uint32(slotBytes[i*4:]))
		if storedSlot != slots[i] {
			return false, nil
		}
	}

	return true, nil
}

// ClearBloomFilter initializes the Bloom filter
func (op *Operator) ClearBloomFilter(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// Get metadata
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// Delete all items
	prefix := string(MakeBloomFilterEntryKey(bfd.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		return op.delete(k)
	})
	if err != nil {
		return fmt.Errorf("failed to clear items: %w", err)
	}

	// Reset Count
	bfd.Count = 0
	err = df.SetBloomFilter(bfd)
	if err != nil {
		return fmt.Errorf("failed to update bloom filter data: %w", err)
	}

	return op.set(key, df)
}

// CountBloomFilter returns the number of elements in Bloom filter
func (op *Operator) CountBloomFilter(key string) (uint64, error) {
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

// DeleteBloomFilter completely deletes the Bloom filter
func (op *Operator) DeleteBloomFilter(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteBloomFilter(key)
}

func (op *Operator) deleteBloomFilter(key string) error {
	// Get metadata
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("bloom filter %s does not exist: %w", key, err)
	}

	bfd, err := df.BloomFilter()
	if err != nil {
		return fmt.Errorf("failed to get bloom filter data: %w", err)
	}

	// Delete all items
	prefix := string(MakeBloomFilterEntryKey(bfd.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		return op.delete(k)
	})
	if err != nil {
		return fmt.Errorf("failed to delete items: %w", err)
	}

	// Delete metadata
	return op.delete(key)
}

// getBloomFilterSlots calculates hash slots for element
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



