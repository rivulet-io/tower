package op

import (
	"fmt"
	"sort"
	"testing"
)

func TestSetBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test_set"

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// ??존재 ?�인
	exists, err := tower.ExistsSet(key)
	if err != nil {
		t.Fatalf("Failed to check set existence: %v", err)
	}
	if !exists {
		t.Error("Expected set to exist")
	}

	// 초기 ?�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected empty set cardinality 0, got %d", cardinality)
	}

	// ????��
	if err := tower.DeleteSet(key); err != nil {
		t.Fatalf("Failed to delete set: %v", err)
	}

	// ??�� ??존재 ?�인
	exists, err = tower.ExistsSet(key)
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추�? ?�스??(SetAdd???�재 Set???�체 ?�기�?반환)
	totalCount, err := tower.AddSetMember(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1, got %d", totalCount)
	}

	// 중복 멤버 추�? ?�도 (Set?� 중복???�용?��? ?�음)
	totalCount, err = tower.AddSetMember(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add duplicate member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 for duplicate, got %d", totalCount)
	}

	// ?�른 멤버??추�?
	members := []PrimitiveString{"member2", "member3", "member4"}
	for _, member := range members {
		tower.AddSetMember(key, member)
	}

	// 최종 ?�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 4 {
		t.Errorf("Expected cardinality 4, got %d", cardinality)
	}

	// 멤버 ?�거 ?�스??(SetRemove???�재 Set???�체 ?�기�?반환)
	remainingCount, err := tower.RemoveSetMember(key, PrimitiveString("member2"))
	if err != nil {
		t.Fatalf("Failed to remove member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3, got %d", remainingCount)
	}

	// 존재?��? ?�는 멤버 ?�거 ?�도
	remainingCount, err = tower.RemoveSetMember(key, PrimitiveString("nonexistent"))
	if err != nil {
		t.Fatalf("Failed to remove nonexistent member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3 for nonexistent, got %d", remainingCount)
	}

	// ?�거 ???�기 ?�인
	cardinality, err = tower.GetSetCardinality(key)
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추�? (Set?� string ?�?�만 지??
	members := []PrimitiveData{
		PrimitiveString("string_member"),
		PrimitiveString("42"),
		PrimitiveString("3.14"),
		PrimitiveString("true"),
	}
	for _, member := range members {
		tower.AddSetMember(key, member)
	}

	// 멤버???�인 ?�스??
	for _, member := range members {
		isMember, err := tower.ContainsSetMember(key, member)
		if err != nil {
			t.Fatalf("Failed to check membership for %v: %v", member, err)
		}
		if !isMember {
			t.Errorf("Expected %v to be a member", member)
		}
	}

	// 존재?��? ?�는 멤버 ?�인
	nonMembers := []PrimitiveData{
		PrimitiveString("nonexistent"),
		PrimitiveString("999"),
		PrimitiveString("false"),
	}
	for _, nonMember := range nonMembers {
		isMember, err := tower.ContainsSetMember(key, nonMember)
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// �??�의 멤버 목록 ?�인
	members, err := tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get members of empty set: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected 0 members in empty set, got %d", len(members))
	}

	// 멤버 추�?
	testMembers := []PrimitiveString{"apple", "banana", "cherry", "date"}
	for _, member := range testMembers {
		tower.AddSetMember(key, member)
	}

	// 멤버 목록 조회
	members, err = tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != len(testMembers) {
		t.Errorf("Expected %d members, got %d", len(testMembers), len(members))
	}

	// 모든 멤버가 ?�함?�어 ?�는지 ?�인
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 멤버 추�?
	for i := 0; i < 10; i++ {
		tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member%d", i)))
	}

	// 추�? ???�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 10 {
		t.Errorf("Expected cardinality 10, got %d", cardinality)
	}

	// ???�리??
	if err := tower.ClearSet(key); err != nil {
		t.Fatalf("Failed to clear set: %v", err)
	}

	// ?�리?????�기 ?�인
	cardinality, err = tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality after clear: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected cardinality 0 after clear, got %d", cardinality)
	}

	// 멤버 목록??비어?�는지 ?�인
	members, err := tower.GetSetMembers(key)
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// ?�양??문자??멤버 추�? (Set?� string ?�?�만 지??
	stringMembers := []PrimitiveData{
		PrimitiveString("string_member"),
		PrimitiveString("100"),
		PrimitiveString("2.71"),
		PrimitiveString("true"),
		PrimitiveString("false"),
		PrimitiveString("binary_data"),
	}

	for i, member := range stringMembers {
		totalCount, err := tower.AddSetMember(key, member)
		if err != nil {
			t.Fatalf("Failed to add member %v: %v", member, err)
		}
		expectedCount := i + 1 // ?�덱??+ 1???�재 Set ?�기
		if totalCount != int64(expectedCount) {
			t.Errorf("Expected total count %d for member %v, got %d", expectedCount, member, totalCount)
		}
	}

	// 최종 ?�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(len(stringMembers)) {
		t.Errorf("Expected cardinality %d, got %d", len(stringMembers), cardinality)
	}

	// �?멤버가 ?�확???�?�되?�는지 ?�인
	for _, expectedMember := range stringMembers {
		isMember, err := tower.ContainsSetMember(key, expectedMember)
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	member := PrimitiveString("duplicate_member")

	// �?번째 추�?
	totalCount, err := tower.AddSetMember(key, member)
	if err != nil {
		t.Fatalf("Failed to add member first time: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 after first add, got %d", totalCount)
	}

	// 중복 추�? ?�도??(Set???�체 ?�기가 반환??
	for i := 0; i < 5; i++ {
		totalCount, err := tower.AddSetMember(key, member)
		if err != nil {
			t.Fatalf("Failed to add duplicate member (attempt %d): %v", i+1, err)
		}
		if totalCount != 1 {
			t.Errorf("Expected total count 1 for duplicate (attempt %d), got %d", i+1, totalCount)
		}
	}

	// 최종 ?�기가 1?��? ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 1 {
		t.Errorf("Expected cardinality 1 after duplicates, got %d", cardinality)
	}

	// 멤버가 ?�전??존재?�는지 ?�인
	isMember, err := tower.ContainsSetMember(key, member)
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

	// 존재?��? ?�는 ?�에 ?�???�업 ?�스??
	_, err := tower.ExistsSet(key)
	if err != nil {
		t.Fatalf("SetExists should not error for non-existent set: %v", err)
	}

	_, err = tower.AddSetMember(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when adding to non-existent set")
	}

	_, err = tower.RemoveSetMember(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when removing from non-existent set")
	}

	_, err = tower.ContainsSetMember(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when checking membership in non-existent set")
	}

	_, err = tower.GetSetMembers(key)
	if err == nil {
		t.Error("Expected error when getting members of non-existent set")
	}

	_, err = tower.GetSetCardinality(key)
	if err == nil {
		t.Error("Expected error when getting cardinality of non-existent set")
	}

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// 중복 ?�성 ?�도
	if err := tower.CreateSet(key); err == nil {
		t.Error("Expected error when creating set that already exists")
	}
}

func TestSetLargeOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "large_set"

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// ?�?�의 멤버 추�?
	memberCount := 100
	for i := 0; i < memberCount; i++ {
		tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_%04d", i)))
	}

	// ?�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(memberCount) {
		t.Errorf("Expected cardinality %d, got %d", memberCount, cardinality)
	}

	// 모든 멤버 조회 �??�렬 ?�인
	members, err := tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != memberCount {
		t.Errorf("Expected %d members, got %d", memberCount, len(members))
	}

	// 멤버?�을 문자?�로 변?�하???�렬
	memberStrings := make([]string, len(members))
	for i, member := range members {
		memberStr, _ := member.String()
		memberStrings[i] = memberStr
	}
	sort.Strings(memberStrings)

	// ?�서?��??�어?�는지 ?�인 (?��? ?�플�?
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

	// ???�성
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	done := make(chan bool, 2)

	// ?�시???�른 멤버??추�?
	go func() {
		for i := 0; i < 50; i++ {
			tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_a_%d", i)))
		}
		done <- true
	}()

	// ?�시???�른 멤버??추�?
	go func() {
		for i := 0; i < 50; i++ {
			tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_b_%d", i)))
		}
		done <- true
	}()

	// ?�료 ?��?
	<-done
	<-done

	// 최종 ?�기 ?�인
	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get final set cardinality: %v", err)
	}
	if cardinality != 100 {
		t.Errorf("Expected cardinality 100, got %d", cardinality)
	}
}

