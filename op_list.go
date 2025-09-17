package tower

import (
	"fmt"
	"math"
)

// 리스트 관리 연산
func (t *Tower) CreateList(key string) error {
	unlock := t.lock(key)
	defer unlock()

	// 리스트 메타데이터를 key에 직접 저장
	listKey := key

	// 이미 존재하는지 확인
	if _, err := t.get(listKey); err == nil {
		return fmt.Errorf("list %s already exists", key)
	}

	// 새로운 리스트 데이터 생성
	listData := &ListData{
		Prefix:    key,
		HeadIndex: 0,
		TailIndex: -1, // 빈 리스트는 TailIndex를 -1로 설정
		Length:    0,
	}

	df := NULLDataFrame()
	if err := df.SetList(listData); err != nil {
		return fmt.Errorf("failed to create list data: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return fmt.Errorf("failed to set list metadata: %w", err)
	}

	return nil
}

func (t *Tower) DeleteList(key string) error {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	// 리스트 메타데이터 가져오기
	df, err := t.get(listKey)
	if err != nil {
		return fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return fmt.Errorf("failed to get list data: %w", err)
	}

	// 모든 아이템 삭제
	for i := listData.HeadIndex; i <= listData.TailIndex; i++ {
		itemKey := string(MakeListItemKey(key, i))
		if err := t.delete(itemKey); err != nil {
			// 아이템이 없어도 계속 진행
			continue
		}
	}

	// 메타데이터 삭제
	if err := t.delete(listKey); err != nil {
		return fmt.Errorf("failed to delete list metadata: %w", err)
	}

	return nil
}

func (t *Tower) ListExists(key string) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key
	_, err := t.get(listKey)
	return err == nil, nil
}

// 기본 Push/Pop 연산
func (t *Tower) PushLeft(key string, value PrimitiveData) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	// 리스트 메타데이터 가져오기
	df, err := t.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	// 아이템 저장 전에 검사
	if listData.Length >= math.MaxInt64-1 {
		return 0, fmt.Errorf("list has too many members")
	}

	// 새로운 인덱스 계산 (왼쪽에 추가하므로 HeadIndex 감소)
	newIndex := listData.HeadIndex - 1

	// DataFrame에 값 설정
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

	// 아이템 저장
	itemKey := string(MakeListItemKey(key, newIndex))
	if err := t.set(itemKey, itemDf); err != nil {
		return 0, fmt.Errorf("failed to set list item: %w", err)
	}

	// 메타데이터 업데이트
	listData.HeadIndex = newIndex
	listData.Length++

	if err := df.SetList(listData); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return listData.Length, nil
}

func (t *Tower) PushRight(key string, value PrimitiveData) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	// 리스트 메타데이터 가져오기
	df, err := t.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	// 아이템 저장 전에 검사
	if listData.Length >= math.MaxInt64-1 {
		return 0, fmt.Errorf("list has too many members")
	}

	// 새로운 인덱스 계산 (오른쪽에 추가하므로 TailIndex 증가)
	newIndex := listData.TailIndex + 1

	// DataFrame에 값 설정
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

	// 아이템 저장
	itemKey := string(MakeListItemKey(key, newIndex))
	if err := t.set(itemKey, itemDf); err != nil {
		return 0, fmt.Errorf("failed to set list item: %w", err)
	}

	// 메타데이터 업데이트
	listData.TailIndex = newIndex
	listData.Length++

	if err := df.SetList(listData); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return 0, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return listData.Length, nil
}

func (t *Tower) PopLeft(key string) (PrimitiveData, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	// 리스트 메타데이터 가져오기
	df, err := t.get(listKey)
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

	// 왼쪽 아이템 가져오기
	itemKey := string(MakeListItemKey(key, listData.HeadIndex))
	itemDf, err := t.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// 값 추출
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

	// 아이템 삭제
	if err := t.delete(itemKey); err != nil {
		return nil, fmt.Errorf("failed to delete list item: %w", err)
	}

	// 메타데이터 업데이트
	listData.HeadIndex++
	listData.Length--

	if err := df.SetList(listData); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return value, nil
}

func (t *Tower) PopRight(key string) (PrimitiveData, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	// 리스트 메타데이터 가져오기
	df, err := t.get(listKey)
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

	// 오른쪽 아이템 가져오기
	itemKey := string(MakeListItemKey(key, listData.TailIndex))
	itemDf, err := t.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// 값 추출
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

	// 아이템 삭제
	if err := t.delete(itemKey); err != nil {
		return nil, fmt.Errorf("failed to delete list item: %w", err)
	}

	// 메타데이터 업데이트
	listData.TailIndex--
	listData.Length--

	if err := df.SetList(listData); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return nil, fmt.Errorf("failed to update list metadata: %w", err)
	}

	return value, nil
}

// 조회 연산
func (t *Tower) ListLength(key string) (int64, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	df, err := t.get(listKey)
	if err != nil {
		return 0, fmt.Errorf("list %s does not exist: %w", key, err)
	}

	listData, err := df.List()
	if err != nil {
		return 0, fmt.Errorf("failed to get list data: %w", err)
	}

	return listData.Length, nil
}

func (t *Tower) ListIndex(key string, index int64) (PrimitiveData, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	df, err := t.get(listKey)
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

	// 인덱스 정규화 (음수 인덱스 지원)
	actualIndex := index
	if index < 0 {
		actualIndex = listData.TailIndex + index + 1
	} else {
		actualIndex = listData.HeadIndex + index
	}

	if actualIndex < listData.HeadIndex || actualIndex > listData.TailIndex {
		return nil, fmt.Errorf("index out of range")
	}

	// 아이템 가져오기
	itemKey := string(MakeListItemKey(key, actualIndex))
	itemDf, err := t.get(itemKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get list item: %w", err)
	}

	// 값 추출
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

func (t *Tower) ListRange(key string, start, end int64) ([]PrimitiveData, error) {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	df, err := t.get(listKey)
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

	// 인덱스 정규화
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

	// 범위 내 아이템들 수집
	// HeadIndex가 underflow/overflow 되었어도 상관없음
	// 0부터 Length-1까지의 상대적 인덱스에 HeadIndex를 더해서 실제 키 계산
	result := make([]PrimitiveData, 0, actualEnd-actualStart+1)
	for i := actualStart; i != actualEnd+1; i++ {
		// 상대적 인덱스 i에 HeadIndex를 더해서 실제 저장된 인덱스 계산
		actualIndex := listData.HeadIndex + i

		itemKey := string(MakeListItemKey(key, actualIndex))
		itemDf, err := t.get(itemKey)
		if err != nil {
			continue // 아이템이 없으면 건너뜀
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

// 수정 연산
func (t *Tower) ListSet(key string, index int64, value PrimitiveData) error {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	df, err := t.get(listKey)
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

	// 인덱스 정규화
	actualIndex := index
	if index < 0 {
		actualIndex = listData.TailIndex + index + 1
	} else {
		actualIndex = listData.HeadIndex + index
	}

	if actualIndex < listData.HeadIndex || actualIndex > listData.TailIndex {
		return fmt.Errorf("index out of range")
	}

	// DataFrame에 값 설정
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

	// 아이템 업데이트
	itemKey := string(MakeListItemKey(key, actualIndex))
	if err := t.set(itemKey, itemDf); err != nil {
		return fmt.Errorf("failed to set list item: %w", err)
	}

	return nil
}

func (t *Tower) ListTrim(key string, start, end int64) error {
	unlock := t.lock(key)
	defer unlock()

	listKey := key

	df, err := t.get(listKey)
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

	// 인덱스 정규화
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
		// 모든 요소 삭제
		for i := listData.HeadIndex; i <= listData.TailIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			t.delete(itemKey)
		}
		listData.HeadIndex = 0
		listData.TailIndex = -1
		listData.Length = 0
	} else {
		// 범위 외 요소들 삭제
		newHeadIndex := listData.HeadIndex + actualStart
		newTailIndex := listData.HeadIndex + actualEnd
		newLength := actualEnd - actualStart + 1

		// 앞부분 삭제
		for i := listData.HeadIndex; i < newHeadIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			t.delete(itemKey)
		}

		// 뒷부분 삭제
		for i := newTailIndex + 1; i <= listData.TailIndex; i++ {
			itemKey := string(MakeListItemKey(key, i))
			t.delete(itemKey)
		}

		listData.HeadIndex = newHeadIndex
		listData.TailIndex = newTailIndex
		listData.Length = newLength
	}

	if err := df.SetList(listData); err != nil {
		return fmt.Errorf("failed to update list metadata: %w", err)
	}

	if err := t.set(listKey, df); err != nil {
		return fmt.Errorf("failed to update list metadata: %w", err)
	}

	return nil
}
