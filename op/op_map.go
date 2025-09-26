package op

import (
	"fmt"
	"math"
	"strings"
)

// Map operations
func (op *Operator) CreateMap(key string) error {
	unlock := op.lock(key)
	defer unlock()

	// Store Map metadata directly to key
	mapKey := key

	// Check if already exists
	if _, err := op.get(mapKey); err == nil {
		return fmt.Errorf("map %s already exists", key)
	}

	// Create new Map data
	mapData := &MapData{
		Prefix: key,
		Count:  0,
	}

	df := NULLDataFrame()
	if err := df.SetMap(mapData); err != nil {
		return fmt.Errorf("failed to create map data: %w", err)
	}

	if err := op.set(mapKey, df); err != nil {
		return fmt.Errorf("failed to set map metadata: %w", err)
	}

	return nil
}

func (op *Operator) DeleteMap(key string) error {
	unlock := op.lock(key)
	defer unlock()

	return op.deleteMap(key)
}

func (op *Operator) deleteMap(key string) error {
	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return fmt.Errorf("failed to get map data: %w", err)
	}

	// Delete all fields
	if mapData.Count > 0 {
		prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
		err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return op.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to delete map fields: %w", err)
		}
	}

	// Delete metadata
	if err := op.delete(mapKey); err != nil {
		return fmt.Errorf("failed to delete map metadata: %w", err)
	}

	return nil
}

func (op *Operator) ExistsMap(key string) (bool, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key
	_, err := op.get(mapKey)
	return err == nil, nil
}

func (op *Operator) SetMapKey(key string, field PrimitiveData, value PrimitiveData) error {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return fmt.Errorf("failed to get map data: %w", err)
	}

	// Generate field key
	fieldStr, err := field.String()
	if err != nil {
		return fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// Check if already exists
	isNew := false
	if _, err := op.get(fieldKey); err != nil {
		isNew = true
	}

	// Check field count (only for new fields)
	if isNew && mapData.Count >= math.MaxUint64-1 {
		return fmt.Errorf("map has too many fields")
	}

	// Set value to DataFrame
	valueDf := NULLDataFrame()
	switch value.Type() {
	case TypeInt:
		intVal, _ := value.Int()
		if err := valueDf.SetInt(intVal); err != nil {
			return fmt.Errorf("failed to set int value: %w", err)
		}
	case TypeFloat:
		floatVal, _ := value.Float()
		if err := valueDf.SetFloat(floatVal); err != nil {
			return fmt.Errorf("failed to set float value: %w", err)
		}
	case TypeString:
		strVal, _ := value.String()
		if err := valueDf.SetString(strVal); err != nil {
			return fmt.Errorf("failed to set string value: %w", err)
		}
	case TypeBool:
		boolVal, _ := value.Bool()
		if err := valueDf.SetBool(boolVal); err != nil {
			return fmt.Errorf("failed to set bool value: %w", err)
		}
	case TypeBinary:
		binVal, _ := value.Binary()
		if err := valueDf.SetBinary(binVal); err != nil {
			return fmt.Errorf("failed to set binary value: %w", err)
		}
	default:
		return fmt.Errorf("unsupported value type")
	}

	// Store value
	if err := op.set(fieldKey, valueDf); err != nil {
		return fmt.Errorf("failed to set map field: %w", err)
	}

	if isNew {
		mapData.Count++

		if err := df.SetMap(mapData); err != nil {
			return fmt.Errorf("failed to update map metadata: %w", err)
		}

		if err := op.set(mapKey, df); err != nil {
			return fmt.Errorf("failed to update map metadata: %w", err)
		}
	}

	return nil
}

func (op *Operator) GetMapKey(key string, field PrimitiveData) (PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return nil, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	_, err = df.Map()
	if err != nil {
		return nil, fmt.Errorf("failed to get map data: %w", err)
	}

	// Generate field key
	fieldStr, err := field.String()
	if err != nil {
		return nil, fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// Get value
	valueDf, err := op.get(fieldKey)
	if err != nil {
		return nil, fmt.Errorf("field does not exist: %w", err)
	}

	// Extract value
	var value PrimitiveData
	switch valueDf.Type() {
	case TypeInt:
		intVal, _ := valueDf.Int()
		value = PrimitiveInt(intVal)
	case TypeFloat:
		floatVal, _ := valueDf.Float()
		value = PrimitiveFloat(floatVal)
	case TypeString:
		strVal, _ := valueDf.String()
		value = PrimitiveString(strVal)
	case TypeBool:
		boolVal, _ := valueDf.Bool()
		value = PrimitiveBool(boolVal)
	case TypeBinary:
		binVal, _ := valueDf.Binary()
		value = PrimitiveBinary(binVal)
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	return value, nil
}

func (op *Operator) DeleteMapKey(key string, field PrimitiveData) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return 0, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return 0, fmt.Errorf("failed to get map data: %w", err)
	}

	// Generate field key
	fieldStr, err := field.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// Check if exists
	if _, err := op.get(fieldKey); err != nil {
		return int64(mapData.Count), nil // No count change if not exists
	}

	// Delete field
	if err := op.delete(fieldKey); err != nil {
		return 0, fmt.Errorf("failed to delete map field: %w", err)
	}

	// Update metadata
	mapData.Count--

	if err := df.SetMap(mapData); err != nil {
		return 0, fmt.Errorf("failed to update map metadata: %w", err)
	}

	if err := op.set(mapKey, df); err != nil {
		return 0, fmt.Errorf("failed to update map metadata: %w", err)
	}

	return int64(mapData.Count), nil
}

func (op *Operator) GetMapKeys(key string) ([]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return nil, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return nil, fmt.Errorf("failed to get map data: %w", err)
	}

	if mapData.Count == 0 {
		return []PrimitiveData{}, nil
	}

	// Collect all keys
	result := make([]PrimitiveData, 0, mapData.Count)
	prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
	err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
		parts := strings.Split(k, ": {:map:} :")
		if len(parts) != 2 {
			return nil // skip invalid key
		}
		fieldStr := parts[1]

		value := PrimitiveString(fieldStr)
		result = append(result, value)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to range map keys: %w", err)
	}

	return result, nil
}

func (op *Operator) GetMapValues(key string) ([]PrimitiveData, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return nil, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return nil, fmt.Errorf("failed to get map data: %w", err)
	}

	if mapData.Count == 0 {
		return []PrimitiveData{}, nil
	}

	// Collect all values
	result := make([]PrimitiveData, 0, mapData.Count)
	prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
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
		return nil, fmt.Errorf("failed to range map values: %w", err)
	}

	return result, nil
}

func (op *Operator) GetMapLength(key string) (int64, error) {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	df, err := op.get(mapKey)
	if err != nil {
		return 0, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return 0, fmt.Errorf("failed to get map data: %w", err)
	}

	return int64(mapData.Count), nil
}

func (op *Operator) ClearMap(key string) error {
	unlock := op.lock(key)
	defer unlock()

	mapKey := key

	// Get Map metadata
	df, err := op.get(mapKey)
	if err != nil {
		return fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return fmt.Errorf("failed to get map data: %w", err)
	}

	// Delete all fields
	if mapData.Count > 0 {
		prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
		err = op.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return op.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to clear map fields: %w", err)
		}
	}

	mapData.Count = 0

	if err := df.SetMap(mapData); err != nil {
		return fmt.Errorf("failed to update map metadata: %w", err)
	}

	if err := op.set(mapKey, df); err != nil {
		return fmt.Errorf("failed to update map metadata: %w", err)
	}

	return nil
}



