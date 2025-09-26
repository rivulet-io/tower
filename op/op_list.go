package op

import (
	"fmt"
	"math"
)

// List management operations
func (op *Operator) CreateList(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// Store list metadata directly to key
	listKey := key

	// Check if already exists
	if _, err := op.get(listKey); err == nil {
		return fmt.Errorf("list %s already exists", key)
	}

	// Create new list data
	listData := &ListData{
		Prefix:    key,
		HeadIndex: 0,
		TailIndex: -1, // Empty list sets TailIndex to -1
		Length:    0,
	}

	df := NULLDataFrame()
	if err := df.SetList(listData); err != nil {
		return fmt.Errorf("failed to create list data: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return fmt.Errorf("failed to set list metadata: %w", err)
	}

	return nil
}

func (op *Operator) DeleteList(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteList(key)
}

func (op *Operator) deleteList(key string) error {
	// Get list metadata
	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return fmt.Errorf("failed to get list data: %w", err)
	}

	// Delete all items
	for i := listData.HeadIndex; i <= listData.TailIndex; i++ {
		itemKey := string(MakeListItemKey(key, i))
		if err := op.delete(itemKey); err != nil {
			// Continue even if no item
			continue
		}
	}

	// Delete metadata
	if err := op.delete(key); err != nil {
		return fmt.Errorf("failed to delete list metadata: %w", err)
	}

	return nil
}

func (op *Operator) ExistsList(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key
	_, err := op.get(listKey)
	return err == nil, nil
}

// Basic Push/Pop operations
func (op *Operator) PushLeftList(key string, value PrimitiveData) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	// Get list metadata
	df, err := op.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length >= math.MaxInt64-1 {
		return 0, fmt.Errorf("list has too many members")
	}

	// Calculate new index (decrease HeadIndex for left addition)
	newIndex := listData.HeadIndex - 1

	// Set value to DataFrame
	itemDf := NULLDataFrame()
	switch value.Type() {
	case TypeInt:
		intVal, _ := value.Int()
		if err := itemDf.SetInt(intVal); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := value.Float()
		if err := itemDf.SetFloat(floatVal); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := value.String()
		if err := itemDf.SetString(strVal); err != nil {
			return 0, fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := value.Bool()
		if err := itemDf.SetBool(boolVal); err != nil {
			return 0, fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeBinary:
		binVal, _ := value.Binary()
		if err := itemDf.SetBinary(binVal); err != nil {
			return 0, fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return 0, fmt.Errorf("unsupported value type")
	}

	// Store item
	itemKey := string(MakeListItemKey(key, newIndex))
	if err := op.set(itemKey, itemDf); err != nil {
		return 0, fmt.Errorf("failed to set list item: %w", err)
	}

	// Update metadata
	listData.HeadIndex = newIndex
	listData.Length++

	if err := df.SetList(listData); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return listData.Length, nil
}

func (op *Operator) PushRightList(key string, value PrimitiveData) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	// Get list metadata
	df, err := op.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length >= math.MaxInt64-1 {
		return 0, fmt.Errorf("list has too many members")
	}

	// Calculate new index (increase TailIndex for right addition)
	newIndex := listData.TailIndex + 1

	// Set value to DataFrame
	itemDf := NULLDataFrame()
	switch value.Type() {
	case TypeInt:
		intVal, _ := value.Int()
		if err := itemDf.SetInt(intVal); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := value.Float()
		if err := itemDf.SetFloat(floatVal); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := value.String()
		if err := itemDf.SetString(strVal); err != nil {
			return 0, fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := value.Bool()
		if err := itemDf.SetBool(boolVal); err != nil {
			return 0, fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeBinary:
		binVal, _ := value.Binary()
		if err := itemDf.SetBinary(binVal); err != nil {
			return 0, fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return 0, fmt.Errorf("unsupported value type")
	}

	// Store item
	itemKey := string(MakeListItemKey(key, newIndex))
	if err := op.set(itemKey, itemDf); err != nil {
		return 0, fmt.Errorf("failed to set list item: %w", err)
	}

	// Update metadata
	listData.TailIndex = newIndex
	listData.Length++

	if err := df.SetList(listData); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return listData.Length, nil
}

func (op *Operator) PopLeftList(key string) (PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	// Get list metadata
	df, err := op.get(listKey)
	if err != nil {
		return nil, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return nil, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return nil, fmt.Errorf("list is empty")
	}

	// Get left item
	itemKey := string(MakeListItemKey(key, listData.HeadIndex))
	itemDf, err := op.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// Extract value
	var value PrimitiveData
	switch itemDf.Type() {
	case TypeInt:
		intVal, _ := itemDf.Int()
		value = PrimitiveInt(intVal)
	case TypeFloat:
		floatVal, _ := itemDf.Float()
		value = PrimitiveFloat(floatVal)
	case TypeString:
		strVal, _ := itemDf.String()
		value = PrimitiveString(strVal)
	case TypeBool:
		boolVal, _ := itemDf.Bool()
		value = PrimitiveBool(boolVal)
	case TypeBinary:
		binVal, _ := itemDf.Binary()
		value = PrimitiveBinary(binVal)
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	// Delete item
	if err := op.delete(itemKey); err != nil {
		return nil, fmt.Errorf("failed to delete list item: %w", err)
	}

	// Update metadata
	listData.HeadIndex++
	listData.Length--

	if err := df.SetList(listData); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return value, nil
}

func (op *Operator) PopRightList(key string) (PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	// Get list metadata
	df, err := op.get(listKey)
	if err != nil {
		return nil, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return nil, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return nil, fmt.Errorf("list is empty")
	}

	// Get right item
	itemKey := string(MakeListItemKey(key, listData.TailIndex))
	itemDf, err := op.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// Extract value
	var value PrimitiveData
	switch itemDf.Type() {
	case TypeInt:
		intVal, _ := itemDf.Int()
		value = PrimitiveInt(intVal)
	case TypeFloat:
		floatVal, _ := itemDf.Float()
		value = PrimitiveFloat(floatVal)
	case TypeString:
		strVal, _ := itemDf.String()
		value = PrimitiveString(strVal)
	case TypeBool:
		boolVal, _ := itemDf.Bool()
		value = PrimitiveBool(boolVal)
	case TypeBinary:
		binVal, _ := itemDf.Binary()
		value = PrimitiveBinary(binVal)
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	// Delete item
	if err := op.delete(itemKey); err != nil {
		return nil, fmt.Errorf("failed to delete list item: %w", err)
	}

	// Update metadata
	listData.TailIndex--
	listData.Length--

	if err := df.SetList(listData); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return value, nil
}

// Query operations
func (op *Operator) GetListLength(key string) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	df, err := op.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	return listData.Length, nil
}

func (op *Operator) GetListIndex(key string, index int64) (PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	df, err := op.get(listKey)
	if err != nil {
		return nil, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return nil, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return nil, fmt.Errorf("list is empty")
	}

	// Normalize index (support negative index)
	actualIndex := index
	if index < 0 {
		actualIndex = listData.TailIndex + index + 1
	} else {
		actualIndex = listData.HeadIndex + index
	}

	if actualIndex < listData.HeadIndex || actualIndex > listData.TailIndex {
		return nil, fmt.Errorf("index out of range")
	}

	// Get item
	itemKey := string(MakeListItemKey(key, actualIndex))
	itemDf, err := op.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// Extract value
	var value PrimitiveData
	switch itemDf.Type() {
	case TypeInt:
		intVal, _ := itemDf.Int()
		value = PrimitiveInt(intVal)
	case TypeFloat:
		floatVal, _ := itemDf.Float()
		value = PrimitiveFloat(floatVal)
	case TypeString:
		strVal, _ := itemDf.String()
		value = PrimitiveString(strVal)
	case TypeBool:
		boolVal, _ := itemDf.Bool()
		value = PrimitiveBool(boolVal)
	case TypeBinary:
		binVal, _ := itemDf.Binary()
		value = PrimitiveBinary(binVal)
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	return value, nil
}

func (op *Operator) GetListRange(key string, start, end int64) ([]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	return op.listRange(key, start, end)
}

func (op *Operator) listRange(key string, start, end int64) ([]PrimitiveData, error) {
	listKey := key

	df, err := op.get(listKey)
	if err != nil {
		return nil, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return nil, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return []PrimitiveData{}, nil
	}

	// Normalize index
	actualStart := start
	actualEnd := end

	if start < 0 {
		actualStart = listData.Length + start
	}
	if end < 0 {
		actualEnd = listData.Length + end
	}

	if actualStart < 0 {
		actualStart = 0
	}
	if actualEnd >= listData.Length {
		actualEnd = listData.Length - 1
	}

	if actualStart > actualEnd {
		return []PrimitiveData{}, nil
	}

	// Collect items in range
	result := make([]PrimitiveData, 0, actualEnd-actualStart+1)
	for i := actualStart; i != actualEnd+1; i++ {
		// Calculate actual stored index by adding HeadIndex to relative index i
		actualIndex := listData.HeadIndex + i

		itemKey := string(MakeListItemKey(key, actualIndex))
		itemDf, err := op.get(itemKey)
		if err != nil {
			continue // Skip if no item
		}

		var value PrimitiveData
		switch itemDf.Type() {
		case TypeInt:
			intVal, _ := itemDf.Int()
			value = PrimitiveInt(intVal)
		case TypeFloat:
			floatVal, _ := itemDf.Float()
			value = PrimitiveFloat(floatVal)
		case TypeString:
			strVal, _ := itemDf.String()
			value = PrimitiveString(strVal)
		case TypeBool:
			boolVal, _ := itemDf.Bool()
			value = PrimitiveBool(boolVal)
		case TypeBinary:
			binVal, _ := itemDf.Binary()
			value = PrimitiveBinary(binVal)
		default:
			continue
		}

		result = append(result, value)
	}

	return result, nil
}

// Update operations
func (op *Operator) SetListIndex(key string, index int64, value PrimitiveData) error {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	df, err := op.get(listKey)
	if err != nil {
		return fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return fmt.Errorf("list is empty")
	}

	// Normalize index
	actualIndex := index
	if index < 0 {
		actualIndex = listData.TailIndex + index + 1
	} else {
		actualIndex = listData.HeadIndex + index
	}

	if actualIndex < listData.HeadIndex || actualIndex > listData.TailIndex {
		return fmt.Errorf("index out of range")
	}

	// Set value to DataFrame
	itemDf := NULLDataFrame()
	switch value.Type() {
	case TypeInt:
		intVal, _ := value.Int()
		if err := itemDf.SetInt(intVal); err != nil {
			return fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := value.Float()
		if err := itemDf.SetFloat(floatVal); err != nil {
			return fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := value.String()
		if err := itemDf.SetString(strVal); err != nil {
			return fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := value.Bool()
		if err := itemDf.SetBool(boolVal); err != nil {
			return fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeBinary:
		binVal, _ := value.Binary()
		if err := itemDf.SetBinary(binVal); err != nil {
			return fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return fmt.Errorf("unsupported value type")
	}

	// Update item
	itemKey := string(MakeListItemKey(key, actualIndex))
	if err := op.set(itemKey, itemDf); err != nil {
		return fmt.Errorf("failed to set list item: %w", err)
	}

	return nil
}

func (op *Operator) TrimList(key string, start, end int64) error {
	unlock := op.lock(key)
	defer unlock()

	listKey := key

	df, err := op.get(listKey)
	if err != nil {
		return fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return nil
	}

	// Normalize index
	actualStart := start
	actualEnd := end

	if start < 0 {
		actualStart = listData.Length + start
	}
	if end < 0 {
		actualEnd = listData.Length + end
	}

	if actualStart < 0 {
		actualStart = 0
	}
	if actualEnd >= listData.Length {
		actualEnd = listData.Length - 1
	}

	if actualStart > actualEnd {
		// Delete all elements
		for i := listData.HeadIndex; i <= listData.TailIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			op.delete(itemKey)
		}
		listData.HeadIndex = 0
		listData.TailIndex = -1
		listData.Length = 0
	} else {
		// Delete out-of-range elements
		newHeadIndex := listData.HeadIndex + actualStart
		newTailIndex := listData.HeadIndex + actualEnd
		newLength := actualEnd - actualStart + 1

		// Delete front part
		for i := listData.HeadIndex; i < newHeadIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			op.delete(itemKey)
		}

		// Delete back part
		for i := newTailIndex + 1; i <= listData.TailIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			op.delete(itemKey)
		}

		listData.HeadIndex = newHeadIndex
		listData.TailIndex = newTailIndex
		listData.Length = newLength
	}

	if err := df.SetList(listData); err != nil {
		return fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := op.set(listKey, df); err != nil {
		return fmt.Errorf("failed to update list metadata: %w", err)
	}

	return nil
}

func (op *Operator) GetAllListMembersAndDelete(key string) ([]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return nil, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return nil, fmt.Errorf("failed to get list data: %w", err)
	}

	if listData.Length == 0 {
		return []PrimitiveData{}, nil
	}

	members, err := op.listRange(key, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to get list members: %w", err)
	}

	if err := op.deleteList(key); err != nil {
		return nil, fmt.Errorf("failed to delete list: %w", err)
	}

	return members, nil
}
