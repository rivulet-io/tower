package tower

import (
	"fmt"
	"sort"
	"testing"
)

func TestSetBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 셋 존재 확인
	exists, err := tower.SetExists(key)
	if err != nil {
		t.Fatalf("Failed to check set existence: %v", err)
	}
	if !exists {
		t.Error("Expected set to exist")
	}

	// 초기 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected empty set cardinality 0, got %d", cardinality)
	}

	// 셋 삭제
	if err := tower.DeleteSet(key); err != nil {
		t.Fatalf("Failed to delete set: %v", err)
	}

	// 삭제 후 존재 확인
	exists, err = tower.SetExists(key)
	if err != nil {
		t.Fatalf("Failed to check set existence after delete: %v", err)
	}
	if exists {
		t.Error("Expected set to not exist after deletion")
	}
}

func TestSetAddAndRemove(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추가 테스트 (SetAdd는 현재 Set의 전체 크기를 반환)
	totalCount, err := tower.SetAdd(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1, got %d", totalCount)
	}

	// 중복 멤버 추가 시도 (Set은 중복을 허용하지 않음)
	totalCount, err = tower.SetAdd(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add duplicate member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 for duplicate, got %d", totalCount)
	}

	// 다른 멤버들 추가
	members := []PrimitiveString{"member2", "member3", "member4"}
	for _, member := range members {
		tower.SetAdd(key, member)
	}

	// 최종 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 4 {
		t.Errorf("Expected cardinality 4, got %d", cardinality)
	}

	// 멤버 제거 테스트 (SetRemove는 현재 Set의 전체 크기를 반환)
	remainingCount, err := tower.SetRemove(key, PrimitiveString("member2"))
	if err != nil {
		t.Fatalf("Failed to remove member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3, got %d", remainingCount)
	}

	// 존재하지 않는 멤버 제거 시도
	remainingCount, err = tower.SetRemove(key, PrimitiveString("nonexistent"))
	if err != nil {
		t.Fatalf("Failed to remove nonexistent member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3 for nonexistent, got %d", remainingCount)
	}

	// 제거 후 크기 확인
	cardinality, err = tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality after remove: %v", err)
	}
	if cardinality != 3 {
		t.Errorf("Expected cardinality 3 after remove, got %d", cardinality)
	}
}

func TestSetMembershipCheck(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추가 (Set은 string 타입만 지원)
	members := []PrimitiveData{
		PrimitiveString("string_member"),
		PrimitiveString("42"),
		PrimitiveString("3.14"),
		PrimitiveString("true"),
	}
	for _, member := range members {
		tower.SetAdd(key, member)
	}

	// 멤버십 확인 테스트
	for _, member := range members {
		isMember, err := tower.SetIsMember(key, member)
		if err != nil {
			t.Fatalf("Failed to check membership for %v: %v", member, err)
		}
		if !isMember {
			t.Errorf("Expected %v to be a member", member)
		}
	}

	// 존재하지 않는 멤버 확인
	nonMembers := []PrimitiveData{
		PrimitiveString("nonexistent"),
		PrimitiveString("999"),
		PrimitiveString("false"),
	}
	for _, nonMember := range nonMembers {
		isMember, err := tower.SetIsMember(key, nonMember)
		if err != nil {
			t.Fatalf("Failed to check membership for %v: %v", nonMember, err)
		}
		if isMember {
			t.Errorf("Expected %v to not be a member", nonMember)
		}
	}
}

func TestSetMembers(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 빈 셋의 멤버 목록 확인
	members, err := tower.SetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get members of empty set: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected 0 members in empty set, got %d", len(members))
	}

	// 멤버 추가
	testMembers := []PrimitiveString{"apple", "banana", "cherry", "date"}
	for _, member := range testMembers {
		tower.SetAdd(key, member)
	}

	// 멤버 목록 조회
	members, err = tower.SetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != len(testMembers) {
		t.Errorf("Expected %d members, got %d", len(testMembers), len(members))
	}

	// 모든 멤버가 포함되어 있는지 확인
	memberSet := make(map[string]bool)
	for _, member := range members {
		if strMember, err := member.String(); err == nil {
			memberSet[strMember] = true
		}
	}

	for _, expectedMember := range testMembers {
		if !memberSet[string(expectedMember)] {
			t.Errorf("Missing member: %s", string(expectedMember))
		}
	}
}

func TestSetClear(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추가
	for i := 0; i < 10; i++ {
		tower.SetAdd(key, PrimitiveString(fmt.Sprintf("member%d", i)))
	}

	// 추가 후 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 10 {
		t.Errorf("Expected cardinality 10, got %d", cardinality)
	}

	// 셋 클리어
	if err := tower.ClearSet(key); err != nil {
		t.Fatalf("Failed to clear set: %v", err)
	}

	// 클리어 후 크기 확인
	cardinality, err = tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality after clear: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected cardinality 0 after clear, got %d", cardinality)
	}

	// 멤버 목록이 비어있는지 확인
	members, err := tower.SetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get members after clear: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected 0 members after clear, got %d", len(members))
	}
}

func TestSetWithStringTypes(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "mixed_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 다양한 문자열 멤버 추가 (Set은 string 타입만 지원)
	stringMembers := []PrimitiveData{
		PrimitiveString("string_member"),
		PrimitiveString("100"),
		PrimitiveString("2.71"),
		PrimitiveString("true"),
		PrimitiveString("false"),
		PrimitiveString("binary_data"),
	}

	for i, member := range stringMembers {
		totalCount, err := tower.SetAdd(key, member)
		if err != nil {
			t.Fatalf("Failed to add member %v: %v", member, err)
		}
		expectedCount := i + 1 // 인덱스 + 1이 현재 Set 크기
		if totalCount != int64(expectedCount) {
			t.Errorf("Expected total count %d for member %v, got %d", expectedCount, member, totalCount)
		}
	}

	// 최종 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(len(stringMembers)) {
		t.Errorf("Expected cardinality %d, got %d", len(stringMembers), cardinality)
	}

	// 각 멤버가 정확히 저장되었는지 확인
	for _, expectedMember := range stringMembers {
		isMember, err := tower.SetIsMember(key, expectedMember)
		if err != nil {
			t.Fatalf("Failed to check membership for %v: %v", expectedMember, err)
		}
		if !isMember {
			t.Errorf("Expected %v to be a member", expectedMember)
		}
	}
}

func TestSetDuplicateHandling(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "duplicate_test_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	member := PrimitiveString("duplicate_member")

	// 첫 번째 추가
	totalCount, err := tower.SetAdd(key, member)
	if err != nil {
		t.Fatalf("Failed to add member first time: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 after first add, got %d", totalCount)
	}

	// 중복 추가 시도들 (Set의 전체 크기가 반환됨)
	for i := 0; i < 5; i++ {
		totalCount, err := tower.SetAdd(key, member)
		if err != nil {
			t.Fatalf("Failed to add duplicate member (attempt %d): %v", i+1, err)
		}
		if totalCount != 1 {
			t.Errorf("Expected total count 1 for duplicate (attempt %d), got %d", i+1, totalCount)
		}
	}

	// 최종 크기가 1인지 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 1 {
		t.Errorf("Expected cardinality 1 after duplicates, got %d", cardinality)
	}

	// 멤버가 여전히 존재하는지 확인
	isMember, err := tower.SetIsMember(key, member)
	if err != nil {
		t.Fatalf("Failed to check membership: %v", err)
	}
	if !isMember {
		t.Error("Expected member to still exist after duplicate additions")
	}
}

func TestSetErrorCases(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// 존재하지 않는 셋에 대한 작업 테스트
	_, err := tower.SetExists(key)
	if err != nil {
		t.Fatalf("SetExists should not error for non-existent set: %v", err)
	}

	_, err = tower.SetAdd(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when adding to non-existent set")
	}

	_, err = tower.SetRemove(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when removing from non-existent set")
	}

	_, err = tower.SetIsMember(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when checking membership in non-existent set")
	}

	_, err = tower.SetMembers(key)
	if err == nil {
		t.Error("Expected error when getting members of non-existent set")
	}

	_, err = tower.SetCardinality(key)
	if err == nil {
		t.Error("Expected error when getting cardinality of non-existent set")
	}

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 중복 생성 시도
	if err := tower.CreateSet(key); err == nil {
		t.Error("Expected error when creating set that already exists")
	}
}

func TestSetLargeOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "large_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 대량의 멤버 추가
	memberCount := 100
	for i := 0; i < memberCount; i++ {
		tower.SetAdd(key, PrimitiveString(fmt.Sprintf("member_%04d", i)))
	}

	// 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(memberCount) {
		t.Errorf("Expected cardinality %d, got %d", memberCount, cardinality)
	}

	// 모든 멤버 조회 및 정렬 확인
	members, err := tower.SetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != memberCount {
		t.Errorf("Expected %d members, got %d", memberCount, len(members))
	}

	// 멤버들을 문자열로 변환하여 정렬
	memberStrings := make([]string, len(members))
	for i, member := range members {
		memberStr, _ := member.String()
		memberStrings[i] = memberStr
	}
	sort.Strings(memberStrings)

	// 순서대로 되어있는지 확인 (일부 샘플만)
	for i := 0; i < 10; i++ {
		expected := fmt.Sprintf("member_%04d", i)
		if memberStrings[i] != expected {
			t.Errorf("Expected %s at position %d, got %s", expected, i, memberStrings[i])
		}
	}
}

func TestSetConcurrentAccess(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "concurrent_set"

	// 셋 생성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	done := make(chan bool, 2)

	// 동시에 다른 멤버들 추가
	go func() {
		for i := 0; i < 50; i++ {
			tower.SetAdd(key, PrimitiveString(fmt.Sprintf("member_a_%d", i)))
		}
		done <- true
	}()

	// 동시에 다른 멤버들 추가
	go func() {
		for i := 0; i < 50; i++ {
			tower.SetAdd(key, PrimitiveString(fmt.Sprintf("member_b_%d", i)))
		}
		done <- true
	}()

	// 완료 대기
	<-done
	<-done

	// 최종 크기 확인
	cardinality, err := tower.SetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get final set cardinality: %v", err)
	}
	if cardinality != 100 {
		t.Errorf("Expected cardinality 100, got %d", cardinality)
	}
}
