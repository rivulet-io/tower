package tower

import (
	"fmt"
	"math"
	"strings"
)

// Map 연산
func (t *Tower) CreateMap(key string) error {
	unlock := t.lock(key)
	defer unlock()

	// Map 메타데이터를 key에 직접 저장
	mapKey := key

	// 이미 존재하는지 확인
	if _, err := t.get(mapKey); err == nil {
		return fmt.Errorf("map %s already exists", key)
	}

	// 새로운 Map 데이터 생성
	mapData := &MapData{
		Prefix: key,
		Count:  0,
	}

	df := NULLDataFrame()
	if err := df.SetMap(mapData); err != nil {
		return fmt.Errorf("failed to create map data: %w", err)
	}

	if err := t.set(mapKey, df); err != nil {
		return fmt.Errorf("failed to set map metadata: %w", err)
	}

	return nil
}

func (t *Tower) DeleteMap(key string) error {
	unlock := t.lock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
	if err != nil {
		return fmt.Errorf("map %s does not exist: %w", key, err)
	}

	_, err = df.Map()
	if err != nil {
		return fmt.Errorf("failed to get map data: %w", err)
	}

	// 모든 필드 삭제 (하지만 실제로는 필드들을 모르기 때문에 메타데이터만 삭제)
	// 실제 구현에서는 모든 필드를 삭제해야 하지만, 여기서는 메타데이터만 삭제
	if err := t.delete(mapKey); err != nil {
		return fmt.Errorf("failed to delete map metadata: %w", err)
	}

	return nil
}

func (t *Tower) MapExists(key string) (bool, error) {
	unlock := t.rlock(key)
	defer unlock()

	mapKey := key
	_, err := t.get(mapKey)
	return err == nil, nil
}

func (t *Tower) MapSet(key string, field PrimitiveData, value PrimitiveData) error {
	unlock := t.lock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
	if err != nil {
		return fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return fmt.Errorf("failed to get map data: %w", err)
	}

	// 필드 키 생성
	fieldStr, err := field.String()
	if err != nil {
		return fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// 이미 존재하는지 확인
	isNew := false
	if _, err := t.get(fieldKey); err != nil {
		isNew = true
	}

	// 필드 수 검사 (새로운 필드인 경우만)
	if isNew && mapData.Count >= math.MaxUint64-1 {
		return fmt.Errorf("map has too many fields")
	}

	// DataFrame에 값 설정
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

	// 값 저장
	if err := t.set(fieldKey, valueDf); err != nil {
		return fmt.Errorf("failed to set map field: %w", err)
	}

	// 메타데이터 업데이트 (새로운 필드인 경우만 카운트 증가)
	if isNew {
		mapData.Count++

		if err := df.SetMap(mapData); err != nil {
			return fmt.Errorf("failed to update map metadata: %w", err)
		}

		if err := t.set(mapKey, df); err != nil {
			return fmt.Errorf("failed to update map metadata: %w", err)
		}
	}

	return nil
}

func (t *Tower) MapGet(key string, field PrimitiveData) (PrimitiveData, error) {
	unlock := t.rlock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
	if err != nil {
		return nil, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	_, err = df.Map()
	if err != nil {
		return nil, fmt.Errorf("failed to get map data: %w", err)
	}

	// 필드 키 생성
	fieldStr, err := field.String()
	if err != nil {
		return nil, fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// 값 가져오기
	valueDf, err := t.get(fieldKey)
	if err != nil {
		return nil, fmt.Errorf("field does not exist: %w", err)
	}

	// 값 추출
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

func (t *Tower) MapDelete(key string, field PrimitiveData) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
	if err != nil {
		return 0, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return 0, fmt.Errorf("failed to get map data: %w", err)
	}

	// 필드 키 생성
	fieldStr, err := field.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get field string: %w", err)
	}
	fieldKey := string(MakeMapItemKey(key, fieldStr))

	// 존재하는지 확인
	if _, err := t.get(fieldKey); err != nil {
		return int64(mapData.Count), nil // 존재하지 않으면 카운트 변경 없음
	}

	// 필드 삭제
	if err := t.delete(fieldKey); err != nil {
		return 0, fmt.Errorf("failed to delete map field: %w", err)
	}

	// 메타데이터 업데이트
	mapData.Count--

	if err := df.SetMap(mapData); err != nil {
		return 0, fmt.Errorf("failed to update map metadata: %w", err)
	}

	if err := t.set(mapKey, df); err != nil {
		return 0, fmt.Errorf("failed to update map metadata: %w", err)
	}

	return int64(mapData.Count), nil
}

func (t *Tower) MapKeys(key string) ([]PrimitiveData, error) {
	unlock := t.rlock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
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

	// 모든 키 수집
	result := make([]PrimitiveData, 0, mapData.Count)
	prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
	err = t.rangePrefix(prefix, func(k string, df *DataFrame) error {
		// k는 key:{:map:}:field
		// field 추출
		parts := strings.Split(k, ": {:map:} :")
		if len(parts) != 2 {
			return nil // skip invalid key
		}
		fieldStr := parts[1]

		// field를 PrimitiveData로 변환 (string으로 가정)
		value := PrimitiveString(fieldStr)
		result = append(result, value)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to range map keys: %w", err)
	}

	return result, nil
}

func (t *Tower) MapValues(key string) ([]PrimitiveData, error) {
	unlock := t.rlock(key)
	defer unlock()

	mapKey := key

	// Map 메타데이터 가져오기
	df, err := t.get(mapKey)
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

	// 모든 값 수집
	result := make([]PrimitiveData, 0, mapData.Count)
	prefix := string(MakeMapEntryKey(mapData.Prefix)) + ":"
	err = t.rangePrefix(prefix, func(k string, df *DataFrame) error {
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

func (t *Tower) MapLength(key string) (int64, error) {
	unlock := t.rlock(key)
	defer unlock()

	mapKey := key

	df, err := t.get(mapKey)
	if err != nil {
		return 0, fmt.Errorf("map %s does not exist: %w", key, err)
	}

	mapData, err := df.Map()
	if err != nil {
		return 0, fmt.Errorf("failed to get map data: %w", err)
	}

	return int64(mapData.Count), nil
}
