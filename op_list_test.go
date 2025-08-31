package tower

import (
	"fmt"
	"testing"
)

func createTestTower(t *testing.T) *Tower {
	opt := &Options{
		Path:         "test.db",
		BytesPerSync: NewSizeFromBytes(32 * 1024), // 32KB
		CacheSize:    NewSizeFromMegabytes(64),    // 64MB
		MemTableSize: NewSizeFromMegabytes(4),     // 4MB
		FS:           InMemory(),
	}
	tower, err := NewTower(opt)
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	return tower
}

func TestListBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 리스트 생성
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	// 리스트 존재 확인
	exists, err := tower.ListExists(key)
	if err != nil {
		t.Fatalf("Failed to check list existence: %v", err)
	}
	if !exists {
		t.Error("Expected list to exist")
	}

	// 초기 길이 확인
	length, err := tower.ListLength(key)
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected empty list length 0, got %d", length)
	}

	// 리스트 삭제
	if err := tower.DeleteList(key); err != nil {
		t.Fatalf("Failed to delete list: %v", err)
	}

	// 삭제 후 존재 확인
	exists, err = tower.ListExists(key)
	if err != nil {
		t.Fatalf("Failed to check list existence after delete: %v", err)
	}
	if exists {
		t.Error("Expected list to not exist after deletion")
	}
}

func TestListPushPopOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 리스트 생성
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	// PushLeft 테스트
	length, err := tower.PushLeft(key, PrimitiveString("left1"))
	if err != nil {
		t.Fatalf("Failed to push left: %v", err)
	}
	if length != 1 {
		t.Errorf("Expected length 1, got %d", length)
	}

	// PushRight 테스트
	length, err = tower.PushRight(key, PrimitiveString("right1"))
	if err != nil {
		t.Fatalf("Failed to push right: %v", err)
	}
	if length != 2 {
		t.Errorf("Expected length 2, got %d", length)
	}

	// 더 많은 아이템 추가
	tower.PushLeft(key, PrimitiveString("left2"))
	tower.PushRight(key, PrimitiveString("right2"))
	// 현재 순서: [left2, left1, right1, right2]

	// 길이 확인
	length, err = tower.ListLength(key)
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}
	if length != 4 {
		t.Errorf("Expected length 4, got %d", length)
	}

	// PopLeft 테스트
	item, err := tower.PopLeft(key)
	if err != nil {
		t.Fatalf("Failed to pop left: %v", err)
	}
	itemStr, err := item.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if itemStr != "left2" {
		t.Errorf("Expected 'left2', got %v", itemStr)
	}

	// PopRight 테스트
	item, err = tower.PopRight(key)
	if err != nil {
		t.Fatalf("Failed to pop right: %v", err)
	}
	itemStr, err = item.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if itemStr != "right2" {
		t.Errorf("Expected 'right2', got %v", itemStr)
	}

	// 길이 확인
	length, err = tower.ListLength(key)
	if err != nil {
		t.Fatalf("Failed to get list length: %v", err)
	}
	if length != 2 {
		t.Errorf("Expected length 2, got %d", length)
	}
}

func TestListIndexAndRange(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 리스트 생성 및 데이터 추가
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	items := []PrimitiveString{"item0", "item1", "item2", "item3", "item4"}
	for _, item := range items {
		tower.PushRight(key, item)
	}

	// ListIndex 테스트
	for i, expected := range items {
		item, err := tower.ListIndex(key, int64(i))
		if err != nil {
			t.Fatalf("Failed to get item at index %d: %v", i, err)
		}
		itemStr, err := item.String()
		if err != nil {
			t.Fatalf("Failed to convert to string: %v", err)
		}
		if itemStr != string(expected) {
			t.Errorf("Expected '%s' at index %d, got %v", string(expected), i, itemStr)
		}
	}

	// 음수 인덱스 테스트
	item, err := tower.ListIndex(key, -1)
	if err != nil {
		t.Fatalf("Failed to get item at index -1: %v", err)
	}
	itemStr, err := item.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if itemStr != "item4" {
		t.Errorf("Expected 'item4' at index -1, got %v", itemStr)
	}

	// ListRange 테스트
	rangeItems, err := tower.ListRange(key, 1, 3)
	if err != nil {
		t.Fatalf("Failed to get range 1-3: %v", err)
	}
	expected := []string{"item1", "item2", "item3"}
	if len(rangeItems) != len(expected) {
		t.Errorf("Expected %d items, got %d", len(expected), len(rangeItems))
	}
	for i, exp := range expected {
		itemStr, err := rangeItems[i].String()
		if err != nil {
			t.Fatalf("Failed to convert range item to string: %v", err)
		}
		if itemStr != exp {
			t.Errorf("Expected '%v' at position %d, got %v", exp, i, itemStr)
		}
	}

	// 전체 범위 테스트
	allItems, err := tower.ListRange(key, 0, -1)
	if err != nil {
		t.Fatalf("Failed to get full range: %v", err)
	}
	if len(allItems) != 5 {
		t.Errorf("Expected 5 items, got %d", len(allItems))
	}
}

func TestListSetAndModify(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 리스트 생성 및 데이터 추가
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	tower.PushRight(key, PrimitiveString("item0"))
	tower.PushRight(key, PrimitiveString("item1"))
	tower.PushRight(key, PrimitiveString("item2"))

	// ListSet 테스트
	if err := tower.ListSet(key, 1, PrimitiveString("modified_item1")); err != nil {
		t.Fatalf("Failed to set item at index 1: %v", err)
	}

	item, err := tower.ListIndex(key, 1)
	if err != nil {
		t.Fatalf("Failed to get modified item: %v", err)
	}
	itemStr, err := item.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if itemStr != "modified_item1" {
		t.Errorf("Expected 'modified_item1', got %v", itemStr)
	}

	// 음수 인덱스로 설정
	if err := tower.ListSet(key, -1, PrimitiveString("last_modified")); err != nil {
		t.Fatalf("Failed to set item at index -1: %v", err)
	}

	item, err = tower.ListIndex(key, -1)
	if err != nil {
		t.Fatalf("Failed to get last item: %v", err)
	}
	itemStr, err = item.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if itemStr != "last_modified" {
		t.Errorf("Expected 'last_modified', got %v", itemStr)
	}
}

func TestListTrim(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 리스트 생성 및 데이터 추가
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	for i := 0; i < 10; i++ {
		tower.PushRight(key, PrimitiveString(fmt.Sprintf("item%d", i)))
	}

	// Trim 테스트 (2-7 범위만 유지)
	if err := tower.ListTrim(key, 2, 7); err != nil {
		t.Fatalf("Failed to trim list: %v", err)
	}

	// 길이 확인
	length, err := tower.ListLength(key)
	if err != nil {
		t.Fatalf("Failed to get list length after trim: %v", err)
	}
	if length != 6 {
		t.Errorf("Expected length 6 after trim, got %d", length)
	}

	// 내용 확인
	firstItem, err := tower.ListIndex(key, 0)
	if err != nil {
		t.Fatalf("Failed to get first item after trim: %v", err)
	}
	firstStr, err := firstItem.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if firstStr != "item2" {
		t.Errorf("Expected 'item2' as first item, got %v", firstStr)
	}

	lastItem, err := tower.ListIndex(key, -1)
	if err != nil {
		t.Fatalf("Failed to get last item after trim: %v", err)
	}
	lastStr, err := lastItem.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if lastStr != "item7" {
		t.Errorf("Expected 'item7' as last item, got %v", lastStr)
	}
}

func TestListErrorCases(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_list"

	// 존재하지 않는 리스트에 대한 작업 테스트
	_, err := tower.ListExists(key)
	if err != nil {
		t.Fatalf("ListExists should not error for non-existent list: %v", err)
	}

	_, err = tower.PushLeft(key, PrimitiveString("item"))
	if err == nil {
		t.Error("Expected error when pushing to non-existent list")
	}

	_, err = tower.PopLeft(key)
	if err == nil {
		t.Error("Expected error when popping from non-existent list")
	}

	_, err = tower.ListIndex(key, 0)
	if err == nil {
		t.Error("Expected error when accessing index of non-existent list")
	}

	// 리스트 생성
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	// 중복 생성 시도
	if err := tower.CreateList(key); err == nil {
		t.Error("Expected error when creating list that already exists")
	}

	// 빈 리스트에서 pop 시도
	_, err = tower.PopLeft(key)
	if err == nil {
		t.Error("Expected error when popping from empty list")
	}

	_, err = tower.PopRight(key)
	if err == nil {
		t.Error("Expected error when popping from empty list")
	}

	// 잘못된 인덱스 접근
	_, err = tower.ListIndex(key, 0)
	if err == nil {
		t.Error("Expected error when accessing index 0 of empty list")
	}

	// 데이터 추가 후 범위 초과 테스트
	tower.PushRight(key, PrimitiveString("item"))

	_, err = tower.ListIndex(key, 10)
	if err == nil {
		t.Error("Expected error when accessing out-of-bounds index")
	}

	err = tower.ListSet(key, 10, PrimitiveString("new_item"))
	if err == nil {
		t.Error("Expected error when setting out-of-bounds index")
	}
}

func TestListWithDifferentTypes(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "mixed_list"

	// 리스트 생성
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	// 다양한 타입의 데이터 추가
	tower.PushRight(key, PrimitiveString("string_value"))
	tower.PushRight(key, PrimitiveInt(42))
	tower.PushRight(key, PrimitiveFloat(3.14))
	tower.PushRight(key, PrimitiveBool(true))

	// 각 타입이 올바르게 저장되었는지 확인
	stringItem, err := tower.ListIndex(key, 0)
	if err != nil {
		t.Fatalf("Failed to get string item: %v", err)
	}
	stringVal, err := stringItem.String()
	if err != nil {
		t.Fatalf("Failed to convert to string: %v", err)
	}
	if stringVal != "string_value" {
		t.Errorf("Expected 'string_value', got %v", stringVal)
	}

	intItem, err := tower.ListIndex(key, 1)
	if err != nil {
		t.Fatalf("Failed to get int item: %v", err)
	}
	intVal, err := intItem.Int()
	if err != nil {
		t.Fatalf("Failed to convert to int: %v", err)
	}
	if intVal != 42 {
		t.Errorf("Expected 42, got %v", intVal)
	}

	floatItem, err := tower.ListIndex(key, 2)
	if err != nil {
		t.Fatalf("Failed to get float item: %v", err)
	}
	floatVal, err := floatItem.Float()
	if err != nil {
		t.Fatalf("Failed to convert to float: %v", err)
	}
	if floatVal != 3.14 {
		t.Errorf("Expected 3.14, got %v", floatVal)
	}

	boolItem, err := tower.ListIndex(key, 3)
	if err != nil {
		t.Fatalf("Failed to get bool item: %v", err)
	}
	boolVal, err := boolItem.Bool()
	if err != nil {
		t.Fatalf("Failed to convert to bool: %v", err)
	}
	if boolVal != true {
		t.Errorf("Expected true, got %v", boolVal)
	}
}

func TestListConcurrentAccess(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "concurrent_list"

	// 리스트 생성
	if err := tower.CreateList(key); err != nil {
		t.Fatalf("Failed to create list: %v", err)
	}

	done := make(chan bool, 2)

	// 동시에 PushLeft 실행
	go func() {
		for i := 0; i < 10; i++ {
			tower.PushLeft(key, PrimitiveString(fmt.Sprintf("left%d", i)))
		}
		done <- true
	}()

	// 동시에 PushRight 실행
	go func() {
		for i := 0; i < 10; i++ {
			tower.PushRight(key, PrimitiveString(fmt.Sprintf("right%d", i)))
		}
		done <- true
	}()

	// 완료 대기
	<-done
	<-done

	// 최종 길이 확인
	length, err := tower.ListLength(key)
	if err != nil {
		t.Fatalf("Failed to get final list length: %v", err)
	}
	if length != 20 {
		t.Errorf("Expected length 20, got %d", length)
	}
}
