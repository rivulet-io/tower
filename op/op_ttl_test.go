package op

import (
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

// setupTower creates a new in-memory Operator instance for testing
func setupTower(t *testing.T) *Operator {
	t.Helper()
	tower, err := NewOperator(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create in-memory tower: %v", err)
	}
	return tower
}

func TestFloorTTLTimestamp(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	criteria := time.UnixMilli(1000000) // 1 second in milliseconds
	expected := int64(1000000 - (1000000 % ttlPrecision))
	result := tower.floorTTLTimestamp(criteria)
	if result != expected {
		t.Errorf("floorTTLTimestamp failed: expected %d, got %d", expected, result)
	}
}

func TestCeilTTLTimestamp(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	criteria := time.UnixMilli(1000000)
	remainder := 1000000 % ttlPrecision
	expected := int64(1000000)
	if remainder != 0 {
		expected = int64(1000000 + (ttlPrecision - remainder))
	}
	result := tower.ceilTTLTimestamp(criteria)
	if result != expected {
		t.Errorf("ceilTTLTimestamp failed: expected %d, got %d", expected, result)
	}
}

func TestMakeTTLKey(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	timestamp := int64(1234567890)
	expected := ttlBaseKey + ":1234567890"
	result := tower.makeTTLKey(timestamp)
	if result != expected {
		t.Errorf("makeTTLKey failed: expected %s, got %s", expected, result)
	}
}

func TestNow(t *testing.T) {
	// Initialize timer
	InitTimer()
	time.Sleep(100 * time.Millisecond) // Wait for timer to update
	now := Now()
	if now.IsZero() {
		t.Error("Now() returned zero time")
	}
}

func TestSetTTL(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	key := "test_key"
	expireAt := time.Now().Add(1 * time.Hour)

	// First, set a value for the key
	err := tower.SetString(key, "test_value")
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Set TTL
	err = tower.SetTTL(key, expireAt)
	if err != nil {
		t.Errorf("SetTTL failed: %v", err)
	}

	// Verify TTL is set by checking if key exists and has expiration
	df, err := tower.get(key)
	if err != nil {
		t.Errorf("Failed to get key after setting TTL: %v", err)
	}
	if df == nil || df.expiresAt.IsZero() {
		t.Error("TTL was not set properly")
	}
}

func TestDeleteTTL(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	key := "test_key"
	expireAt := time.Now().Add(1 * time.Hour)

	// Set a value and TTL
	err := tower.SetString(key, "test_value")
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = tower.SetTTL(key, expireAt)
	if err != nil {
		t.Fatalf("Failed to set TTL: %v", err)
	}

	// Remove TTL
	err = tower.DeleteTTL(key)
	if err != nil {
		t.Errorf("DeleteTTL failed: %v", err)
	}

	// Verify TTL is removed
	df, err := tower.get(key)
	if err != nil {
		t.Errorf("Failed to get key after removing TTL: %v", err)
	}
	if df == nil || !df.expiresAt.IsZero() {
		t.Error("TTL was not removed properly")
	}
}

func TestTruncateExpired(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	key := "expired_key"
	expireAt := time.Now().Add(1 * time.Second) // Set to near future

	// Set a value and TTL
	err := tower.SetString(key, "test_value")
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = tower.SetTTL(key, expireAt)
	if err != nil {
		t.Fatalf("Failed to set TTL: %v", err)
	}

	// Wait for expiration
	time.Sleep(2 * time.Second)

	// Truncate expired keys
	err = tower.TruncateExpired()
	if err != nil {
		t.Errorf("TruncateExpired failed: %v", err)
	}

	// Verify key is deleted
	df, err := tower.get(key)
	if err == nil && df != nil {
		t.Error("Expired key was not deleted")
	}
}

func TestAddCandidatesForExpiration(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	key := "test_key"
	expireAt := time.Now().Add(1 * time.Hour)

	err := tower.addCandidatesForExpiration(key, expireAt)
	if err != nil {
		t.Errorf("addCandidatesForExpiration failed: %v", err)
	}

	// Verify candidate is added (check internal list)
	ttlKey := tower.makeTTLKey(tower.ceilTTLTimestamp(expireAt))
	members, err := tower.GetListRange(ttlKey, 0, -1)
	if err != nil {
		t.Errorf("Failed to list TTL candidates: %v", err)
	}
	found := false
	for _, member := range members {
		if str, err := member.String(); err == nil && str == key {
			found = true
			break
		}
	}
	if !found {
		t.Error("Key was not added to expiration candidates")
	}
}

func TestExtractCandidatesForExpiration(t *testing.T) {
	tower := setupTower(t)
	defer tower.Close()

	criteria := time.Now()

	candidates, err := tower.extractCandidatesForExpiration(criteria)
	if err != nil {
		t.Errorf("extractCandidatesForExpiration failed: %v", err)
	}

	// Since no keys are set, candidates should be empty
	if len(candidates) != 0 {
		t.Errorf("Expected no candidates, got %d", len(candidates))
	}
}

