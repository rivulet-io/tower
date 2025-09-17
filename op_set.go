package tower

import (
	"fmt"
	"math"
)

// Set 연산
func (t *Tower) CreateSet(key string) error {
	unlock := t.lock(key)
	defer unlock()

	// Set 메타데이터를 key에 직접 저장
	setKey := key

	// 이미 존재하는지 확인
	if _, err := t.get(setKey); err == nil {
		return fmt.Errorf("set %s already exists", key)
	}

	// 새로운 Set 데이터 생성
	setData := &SetData{
		Prefix: key,
		Count:  0,
	}

	df := NULLDataFrame()
	if err := df.SetSet(setData); err != nil {
		return fmt.Errorf("failed to create set data: %w", err)
	}

	if err := t.set(setKey, df); err != nil {
		return fmt.Errorf("failed to set set metadata: %w", err)
	}

	return nil
}

func (t *Tower) DeleteSet(key string) error {
	unlock := t.lock(key)
	defer unlock()

	return t.deleteSet(key)
}

func (t *Tower) deleteSet(key string) error {
	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
	if err != nil {
		return fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return fmt.Errorf("failed to get set data: %w", err)
	}

	// 모든 멤버 삭제
	if setData.Count > 0 {
		prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
		err = t.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return t.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to delete set members: %w", err)
		}
	}

	// 메타데이터 삭제
	if err := t.delete(setKey); err != nil {
		return fmt.Errorf("failed to delete set metadata: %w", err)
	}

	return nil
}

func (t *Tower) SetExists(key string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key
	_, err := t.get(setKey)
	return err == nil, nil
}

func (t *Tower) SetAdd(key string, member PrimitiveData) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	// 멤버 키 생성
	memberStr, err := member.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// 이미 존재하는지 확인
	if _, err := t.get(memberKey); err == nil {
		return int64(setData.Count), nil // 이미 존재하면 카운트 변경 없음
	}

	// 멤버 수 검사
	if setData.Count >= math.MaxUint64-1 {
		return 0, fmt.Errorf("set has too many members")
	}

	// DataFrame에 값 설정
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

	// 멤버 저장
	if err := t.set(memberKey, memberDf); err != nil {
		return 0, fmt.Errorf("failed to set set member: %w", err)
	}

	// 메타데이터 업데이트
	setData.Count++

	if err := df.SetSet(setData); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := t.set(setKey, df); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	return int64(setData.Count), nil
}

func (t *Tower) SetRemove(key string, member PrimitiveData) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	// 멤버 키 생성
	memberStr, err := member.String()
	if err != nil {
		return 0, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// 존재하는지 확인
	if _, err := t.get(memberKey); err != nil {
		return int64(setData.Count), nil // 존재하지 않으면 카운트 변경 없음
	}

	// 멤버 삭제
	if err := t.delete(memberKey); err != nil {
		return 0, fmt.Errorf("failed to delete set member: %w", err)
	}

	// 메타데이터 업데이트
	setData.Count--

	if err := df.SetSet(setData); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := t.set(setKey, df); err != nil {
		return 0, fmt.Errorf("failed to update set metadata: %w", err)
	}

	return int64(setData.Count), nil
}

func (t *Tower) SetIsMember(key string, member PrimitiveData) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
	if err != nil {
		return false, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	_, err = df.Set()
	if err != nil {
		return false, fmt.Errorf("failed to get set data: %w", err)
	}

	// 멤버 키 생성
	memberStr, err := member.String()
	if err != nil {
		return false, fmt.Errorf("failed to get member string: %w", err)
	}
	memberKey := string(MakeSetItemKey(key, memberStr))

	// 존재하는지 확인
	_, err = t.get(memberKey)
	return err == nil, nil
}

func (t *Tower) SetMembers(key string) ([]PrimitiveData, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
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

	// 모든 멤버 수집
	result := make([]PrimitiveData, 0, setData.Count)
	prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
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
		return nil, fmt.Errorf("failed to range set members: %w", err)
	}

	return result, nil
}

func (t *Tower) SetCardinality(key string) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	df, err := t.get(setKey)
	if err != nil {
		return 0, fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return 0, fmt.Errorf("failed to get set data: %w", err)
	}

	return int64(setData.Count), nil
}

func (t *Tower) ClearSet(key string) error {
	unlock := t.lock(key)
	defer unlock()

	setKey := key

	// Set 메타데이터 가져오기
	df, err := t.get(setKey)
	if err != nil {
		return fmt.Errorf("set %s does not exist: %w", key, err)
	}

	setData, err := df.Set()
	if err != nil {
		return fmt.Errorf("failed to get set data: %w", err)
	}

	// 모든 멤버 삭제
	if setData.Count > 0 {
		prefix := string(MakeSetEntryKey(setData.Prefix)) + ":"
		err = t.rangePrefix(prefix, func(k string, df *DataFrame) error {
			return t.delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to clear set members: %w", err)
		}
	}

	// 메타데이터 업데이트 (Count를 0으로 리셋)
	setData.Count = 0

	if err := df.SetSet(setData); err != nil {
		return fmt.Errorf("failed to update set metadata: %w", err)
	}

	if err := t.set(setKey, df); err != nil {
		return fmt.Errorf("failed to update set metadata: %w", err)
	}

	return nil
}
