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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	exists, err := tower.ExistsSet(key)
	if err != nil {
		t.Fatalf("Failed to check set existence: %v", err)
	}
	if !exists {
		t.Error("Expected set to exist")
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected empty set cardinality 0, got %d", cardinality)
	}

	if err := tower.DeleteSet(key); err != nil {
		t.Fatalf("Failed to delete set: %v", err)
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	totalCount, err := tower.AddSetMember(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1, got %d", totalCount)
	}

	totalCount, err = tower.AddSetMember(key, PrimitiveString("member1"))
	if err != nil {
		t.Fatalf("Failed to add duplicate member: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 for duplicate, got %d", totalCount)
	}

	members := []PrimitiveString{"member2", "member3", "member4"}
	for _, member := range members {
		tower.AddSetMember(key, member)
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 4 {
		t.Errorf("Expected cardinality 4, got %d", cardinality)
	}

	remainingCount, err := tower.DeleteSetMember(key, PrimitiveString("member2"))
	if err != nil {
		t.Fatalf("Failed to remove member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3, got %d", remainingCount)
	}

	remainingCount, err = tower.DeleteSetMember(key, PrimitiveString("nonexistent"))
	if err != nil {
		t.Fatalf("Failed to remove nonexistent member: %v", err)
	}
	if remainingCount != 3 {
		t.Errorf("Expected remaining count 3 for nonexistent, got %d", remainingCount)
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	members := []PrimitiveData{
		PrimitiveString("string_member"),
		PrimitiveString("42"),
		PrimitiveString("3.14"),
		PrimitiveString("true"),
	}
	for _, member := range members {
		tower.AddSetMember(key, member)
	}

	for _, member := range members {
		isMember, err := tower.ContainsSetMember(key, member)
		if err != nil {
			t.Fatalf("Failed to check membership for %v: %v", member, err)
		}
		if !isMember {
			t.Errorf("Expected %v to be a member", member)
		}
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	members, err := tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get members of empty set: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected 0 members in empty set, got %d", len(members))
	}

	testMembers := []PrimitiveString{"apple", "banana", "cherry", "date"}
	for _, member := range testMembers {
		tower.AddSetMember(key, member)
	}

	members, err = tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != len(testMembers) {
		t.Errorf("Expected %d members, got %d", len(testMembers), len(members))
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	for i := 0; i < 10; i++ {
		tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member%d", i)))
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 10 {
		t.Errorf("Expected cardinality 10, got %d", cardinality)
	}

	if err := tower.ClearSet(key); err != nil {
		t.Fatalf("Failed to clear set: %v", err)
	}

	cardinality, err = tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality after clear: %v", err)
	}
	if cardinality != 0 {
		t.Errorf("Expected cardinality 0 after clear, got %d", cardinality)
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

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
		expectedCount := i + 1
		if totalCount != int64(expectedCount) {
			t.Errorf("Expected total count %d for member %v, got %d", expectedCount, member, totalCount)
		}
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(len(stringMembers)) {
		t.Errorf("Expected cardinality %d, got %d", len(stringMembers), cardinality)
	}

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	member := PrimitiveString("duplicate_member")

	totalCount, err := tower.AddSetMember(key, member)
	if err != nil {
		t.Fatalf("Failed to add member first time: %v", err)
	}
	if totalCount != 1 {
		t.Errorf("Expected total count 1 after first add, got %d", totalCount)
	}

	for i := 0; i < 5; i++ {
		totalCount, err := tower.AddSetMember(key, member)
		if err != nil {
			t.Fatalf("Failed to add duplicate member (attempt %d): %v", i+1, err)
		}
		if totalCount != 1 {
			t.Errorf("Expected total count 1 for duplicate (attempt %d), got %d", i+1, totalCount)
		}
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != 1 {
		t.Errorf("Expected cardinality 1 after duplicates, got %d", cardinality)
	}

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

	_, err := tower.ExistsSet(key)
	if err != nil {
		t.Fatalf("SetExists should not error for non-existent set: %v", err)
	}

	_, err = tower.AddSetMember(key, PrimitiveString("member"))
	if err == nil {
		t.Error("Expected error when adding to non-existent set")
	}

	_, err = tower.DeleteSetMember(key, PrimitiveString("member"))
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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	if err := tower.CreateSet(key); err == nil {
		t.Error("Expected error when creating set that already exists")
	}
}

func TestSetLargeOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "large_set"

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	memberCount := 100
	for i := 0; i < memberCount; i++ {
		tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_%04d", i)))
	}

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get set cardinality: %v", err)
	}
	if cardinality != int64(memberCount) {
		t.Errorf("Expected cardinality %d, got %d", memberCount, cardinality)
	}

	members, err := tower.GetSetMembers(key)
	if err != nil {
		t.Fatalf("Failed to get set members: %v", err)
	}

	if len(members) != memberCount {
		t.Errorf("Expected %d members, got %d", memberCount, len(members))
	}

	memberStrings := make([]string, len(members))
	for i, member := range members {
		memberStr, _ := member.String()
		memberStrings[i] = memberStr
	}
	sort.Strings(memberStrings)

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

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	done := make(chan bool, 2)

	go func() {
		for i := 0; i < 50; i++ {
			tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_a_%d", i)))
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 50; i++ {
			tower.AddSetMember(key, PrimitiveString(fmt.Sprintf("member_b_%d", i)))
		}
		done <- true
	}()

	<-done
	<-done

	cardinality, err := tower.GetSetCardinality(key)
	if err != nil {
		t.Fatalf("Failed to get final set cardinality: %v", err)
	}
	if cardinality != 100 {
		t.Errorf("Expected cardinality 100, got %d", cardinality)
	}
}

func TestSetMembersFiltered(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "filtered_test_set"

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// Test filtering on empty set
	filteredMembers, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		return true
	})
	if err != nil {
		t.Fatalf("Failed to get filtered members of empty set: %v", err)
	}
	if len(filteredMembers) != 0 {
		t.Errorf("Expected 0 filtered members in empty set, got %d", len(filteredMembers))
	}

	// Add diverse test data
	testMembers := []PrimitiveData{
		PrimitiveString("apple"),
		PrimitiveString("banana"),
		PrimitiveString("cherry"),
		PrimitiveString("date"),
		PrimitiveString("elderberry"),
		PrimitiveString("fig"),
		PrimitiveString("grape"),
	}

	for _, member := range testMembers {
		if _, err := tower.AddSetMember(key, member); err != nil {
			t.Fatalf("Failed to add member %v: %v", member, err)
		}
	}

	// Test filter that accepts all members
	allMembers, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		return true
	})
	if err != nil {
		t.Fatalf("Failed to get all filtered members: %v", err)
	}
	if len(allMembers) != len(testMembers) {
		t.Errorf("Expected %d members with accept-all filter, got %d", len(testMembers), len(allMembers))
	}

	// Test filter that rejects all members
	noMembers, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get filtered members with reject-all filter: %v", err)
	}
	if len(noMembers) != 0 {
		t.Errorf("Expected 0 members with reject-all filter, got %d", len(noMembers))
	}

	// Test filter for strings starting with specific letter
	startsWithA, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		if str, err := data.String(); err == nil {
			return len(str) > 0 && str[0] == 'a'
		}
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get members starting with 'a': %v", err)
	}
	if len(startsWithA) != 1 {
		t.Errorf("Expected 1 member starting with 'a', got %d", len(startsWithA))
	}
	if len(startsWithA) > 0 {
		if str, _ := startsWithA[0].String(); str != "apple" {
			t.Errorf("Expected 'apple', got %s", str)
		}
	}

	// Test filter for strings with specific length
	lengthFive, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		if str, err := data.String(); err == nil {
			return len(str) == 5
		}
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get members with length 5: %v", err)
	}
	expectedCount := 2 // "apple" and "grape"
	if len(lengthFive) != expectedCount {
		t.Errorf("Expected %d members with length 5, got %d", expectedCount, len(lengthFive))
	}

	// Test filter for strings containing specific substring
	containsE, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		if str, err := data.String(); err == nil {
			return len(str) > 0 && str[len(str)-1] == 'e'
		}
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get members ending with 'e': %v", err)
	}
	// Members ending with 'e': "apple", "grape", but also "date" - so 3 total
	expectedEndingE := 3 // "apple", "grape", "date"
	if len(containsE) != expectedEndingE {
		t.Errorf("Expected %d members ending with 'e', got %d", expectedEndingE, len(containsE))
	}
}

func TestSetMembersFilteredWithMixedTypes(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "mixed_filtered_set"

	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// Add mixed type members (as strings since sets store everything as strings in this implementation)
	mixedMembers := []PrimitiveData{
		PrimitiveString("100"),
		PrimitiveString("200"),
		PrimitiveString("hello"),
		PrimitiveString("world"),
		PrimitiveString("42"),
		PrimitiveString("test"),
	}

	for _, member := range mixedMembers {
		if _, err := tower.AddSetMember(key, member); err != nil {
			t.Fatalf("Failed to add member %v: %v", member, err)
		}
	}

	// Filter for numeric-like strings
	numericStrings, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		if str, err := data.String(); err == nil {
			// Simple check if string contains only digits
			for _, r := range str {
				if r < '0' || r > '9' {
					return false
				}
			}
			return len(str) > 0
		}
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get numeric string members: %v", err)
	}
	expectedNumeric := 3 // "100", "200", "42"
	if len(numericStrings) != expectedNumeric {
		t.Errorf("Expected %d numeric string members, got %d", expectedNumeric, len(numericStrings))
	}

	// Filter for alphabetic strings
	alphabeticStrings, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		if str, err := data.String(); err == nil {
			// Simple check if string contains only letters
			for _, r := range str {
				if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
					return false
				}
			}
			return len(str) > 0
		}
		return false
	})
	if err != nil {
		t.Fatalf("Failed to get alphabetic string members: %v", err)
	}
	expectedAlphabetic := 3 // "hello", "world", "test"
	if len(alphabeticStrings) != expectedAlphabetic {
		t.Errorf("Expected %d alphabetic string members, got %d", expectedAlphabetic, len(alphabeticStrings))
	}
}

func TestSetMembersFilteredErrorCases(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "nonexistent_set"

	// Test filtering on non-existent set
	_, err := tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		return true
	})
	if err == nil {
		t.Error("Expected error when filtering members of non-existent set")
	}

	// Create set and test with nil filter (this would panic, so we don't test it)
	if err := tower.CreateSet(key); err != nil {
		t.Fatalf("Failed to create set: %v", err)
	}

	// Test filter that might panic (but shouldn't crash the system)
	_, err = tower.GetSetMembersFiltered(key, func(data PrimitiveData) bool {
		// Safe filter that always returns false
		return false
	})
	if err != nil {
		t.Fatalf("Expected no error with empty set and safe filter: %v", err)
	}
}
