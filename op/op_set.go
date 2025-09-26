package op

import (
	"fmt"
	"math"
)

// Set operations
func (op *Operator) CreateSet(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// Store Set metadata directly to key
	setKey := key

	// Check if already exists
	if _, err := op.get(setKey); err == nil {
		return fmt.Errorf("set %s already exists", key)
	}

	// Create new Set data
	setData := &SetData{
		Prefix: key,
		Count:  0,
	}

	df := NULLDataFrame()
	if err := df.SetSet(setData); err != nil {
		return fmt.Errorf("failed to create set data: %w", err)
	}

	if err := op.set(setKey, df); err != nil {
		return fmt.Errorf("failed to set set metadata: %w", err)
	}

	return nil
}

func (op *Operator) DeleteSet(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteSet(key)
}

func (op *Operator) deleteSet(key string) error {
	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return fmt.Errorf("failed to get set data: %w", err)
	}

	// Delete all members
	if setData.Count > 0 {
		prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
		err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return op.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to delete set members: %w", err)
		}
	}

	// Delete metadata
	if err := op.delete(setKey); err != nil {
		return fmt.Errorf("failed to delete set metadata: %w", err)
	}

	return nil
}

func (op *Operator) ExistsSet(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key
	_, err := op.get(setKey)
	return err == nil, nil
}

func (op *Operator) AddSetMember(key string, member PrimitiveData) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	// Generate member key
	memberStr, err := member.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// Check if already exists
	if _, err := op.get(memberKey); err == nil {
		return int64(setData.Count), nil // No count change if already exists
	}

	// Check member count
	if setData.Count >= math.MaxUint64-1 {
		return 0, fmt.Errorf("set has too many members")
	}

	// Set value to DataFrame
	memberDf := NULLDataFrame()
	switch member.Type() {
	case TypeInt:
		intVal, _ := member.Int()
		if err := memberDf.SetInt(intVal); err != nil {
			return 0, fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := member.Float()
		if err := memberDf.SetFloat(floatVal); err != nil {
			return 0, fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := member.String()
		if err := memberDf.SetString(strVal); err != nil {
			return 0, fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := member.Bool()
		if err := memberDf.SetBool(boolVal); err != nil {
			return 0, fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeBinary:
		binVal, _ := member.Binary()
		if err := memberDf.SetBinary(binVal); err != nil {
			return 0, fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return 0, fmt.Errorf("unsupported value type")
	}

	// Store member
	if err := op.set(memberKey, memberDf); err != nil {
		return 0, fmt.Errorf("failed to set set member: %w", err)
	}

	// Update metadata
	setData.Count++

	if err := df.SetSet(setData); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := op.set(setKey, df); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	return int64(setData.Count), nil
}

func (op *Operator) RemoveSetMember(key string, member PrimitiveData) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	// Generate member key
	memberStr, err := member.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// Check if exists
	if _, err := op.get(memberKey); err != nil {
		return int64(setData.Count), nil // No count change if not exists
	}

	// Delete member
	if err := op.delete(memberKey); err != nil {
		return 0, fmt.Errorf("failed to delete set member: %w", err)
	}

	// Update metadata
	setData.Count--

	if err := df.SetSet(setData); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := op.set(setKey, df); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	return int64(setData.Count), nil
}

func (op *Operator) ContainsSetMember(key string, member PrimitiveData) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return false, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	_, err = df.Set()
	if err != nil {
		return false, fmt.Errorf("failed to get set data: %w", err)
	}

	// Generate member key
	memberStr, err := member.String()
	if err != nil {
		return false, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// Check if exists
	_, err = op.get(memberKey)
	return err == nil, nil
}

func (op *Operator) GetSetMembers(key string) ([]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return nil, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return nil, fmt.Errorf("failed to get set data: %w", err)
	}

	if setData.Count == 0 {
		return []PrimitiveData{}, nil
	}

	// Collect all members
	result := make([]PrimitiveData, 0, setData.Count)
	prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		var value PrimitiveData
		switch df.Type() {
		case TypeInt:
			intVal, _ := df.Int()
			value = PrimitiveInt(intVal)
		case TypeFloat:
			floatVal, _ := df.Float()
			value = PrimitiveFloat(floatVal)
		case TypeString:
			strVal, _ := df.String()
			value = PrimitiveString(strVal)
		case TypeBool:
			boolVal, _ := df.Bool()
			value = PrimitiveBool(boolVal)
		case TypeBinary:
			binVal, _ := df.Binary()
			value = PrimitiveBinary(binVal)
		default:
			return nil // skip unsupported types
		}
		result = append(result, value)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to range set members: %w", err)
	}

	return result, nil
}

func (op *Operator) GetSetCardinality(key string) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	df, err := op.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	return int64(setData.Count), nil
}

func (op *Operator) ClearSet(key string) error {
	unlock := op.lock(key)
	defer unlock()

	setKey := key

	// Get Set metadata
	df, err := op.get(setKey)
	if err != nil {
		return fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return fmt.Errorf("failed to get set data: %w", err)
	}

	// Delete all members
	if setData.Count > 0 {
		prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
		err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return op.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to clear set members: %w", err)
		}
	}

	setData.Count = 0

	if err := df.SetSet(setData); err != nil {
		return fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := op.set(setKey, df); err != nil {
		return fmt.Errorf("failed to update set metadata: %w", err)
	}

	return nil
}



