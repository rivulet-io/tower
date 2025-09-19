package op

import (
	"bytes"
	"testing"
)

func TestShamirBasicOperations(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	// Test SplitSecret and CombineShares
	t.Run("SplitSecret_CombineShares", func(t *testing.T) {
		key := "test:shamir:basic"
		secret := []byte("This is a secret message for testing Shamir secret sharing")

		// Split into 5 shares requiring 3 to reconstruct
		shares, err := tower.SplitSecret(key, secret, 5, 3)
		if err != nil {
			t.Fatalf("Failed to split secret: %v", err)
		}

		if len(shares) != 5 {
			t.Errorf("Expected 5 shares, got %d", len(shares))
		}

		// Verify we can retrieve the shares
		retrievedShares, err := tower.GetShamirShare(key)
		if err != nil {
			t.Fatalf("Failed to get Shamir shares: %v", err)
		}

		if len(retrievedShares) != 5 {
			t.Errorf("Expected 5 retrieved shares, got %d", len(retrievedShares))
		}

		// Reconstruct the secret
		reconstructed, err := tower.CombineShares(key)
		if err != nil {
			t.Fatalf("Failed to combine shares: %v", err)
		}

		if !bytes.Equal(reconstructed, secret) {
			t.Errorf("Reconstructed secret doesn't match original. Expected: %s, Got: %s", string(secret), string(reconstructed))
		}
	})

	// Test SetShamirShare and GetShamirShare
	t.Run("SetShamirShare_GetShamirShare", func(t *testing.T) {
		key := "test:shamir:setget"
		testShares := map[byte][]byte{
			1: []byte("share1data"),
			2: []byte("share2data"),
			3: []byte("share3data"),
		}

		err := tower.SetShamirShare(key, testShares)
		if err != nil {
			t.Fatalf("Failed to set Shamir shares: %v", err)
		}

		retrievedShares, err := tower.GetShamirShare(key)
		if err != nil {
			t.Fatalf("Failed to get Shamir shares: %v", err)
		}

		if len(retrievedShares) != len(testShares) {
			t.Errorf("Expected %d shares, got %d", len(testShares), len(retrievedShares))
		}

		for id, expectedShare := range testShares {
			retrievedShare, exists := retrievedShares[id]
			if !exists {
				t.Errorf("Share ID %d not found in retrieved shares", id)
				continue
			}
			if !bytes.Equal(retrievedShare, expectedShare) {
				t.Errorf("Share %d mismatch. Expected: %v, Got: %v", id, expectedShare, retrievedShare)
			}
		}
	})
}

func TestShamirShareManagement(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test:shamir:management"
	secret := []byte("Secret for share management testing")

	// Split secret initially
	_, err := tower.SplitSecret(key, secret, 3, 2)
	if err != nil {
		t.Fatalf("Failed to split secret: %v", err)
	}

	// Test GetShareCount
	t.Run("GetShareCount", func(t *testing.T) {
		count, err := tower.GetShareCount(key)
		if err != nil {
			t.Fatalf("Failed to get share count: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 shares, got %d", count)
		}
	})

	// Test AddShare
	t.Run("AddShare", func(t *testing.T) {
		newShareData := []byte("new share data")
		err := tower.AddShare(key, 100, newShareData)
		if err != nil {
			t.Fatalf("Failed to add share: %v", err)
		}

		count, err := tower.GetShareCount(key)
		if err != nil {
			t.Fatalf("Failed to get share count after add: %v", err)
		}
		if count != 4 {
			t.Errorf("Expected 4 shares after add, got %d", count)
		}

		// Verify the share was added correctly
		retrievedShare, err := tower.GetShare(key, 100)
		if err != nil {
			t.Fatalf("Failed to get added share: %v", err)
		}
		if !bytes.Equal(retrievedShare, newShareData) {
			t.Errorf("Added share data mismatch. Expected: %v, Got: %v", newShareData, retrievedShare)
		}
	})

	// Test HasShare
	t.Run("HasShare", func(t *testing.T) {
		exists, err := tower.HasShare(key, 100)
		if err != nil {
			t.Fatalf("Failed to check share existence: %v", err)
		}
		if !exists {
			t.Error("Expected share 100 to exist")
		}

		exists, err = tower.HasShare(key, 200)
		if err != nil {
			t.Fatalf("Failed to check non-existent share: %v", err)
		}
		if exists {
			t.Error("Expected share 200 to not exist")
		}
	})

	// Test RemoveShare
	t.Run("RemoveShare", func(t *testing.T) {
		err := tower.RemoveShare(key, 100)
		if err != nil {
			t.Fatalf("Failed to remove share: %v", err)
		}

		count, err := tower.GetShareCount(key)
		if err != nil {
			t.Fatalf("Failed to get share count after remove: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected 3 shares after remove, got %d", count)
		}

		exists, err := tower.HasShare(key, 100)
		if err != nil {
			t.Fatalf("Failed to check removed share: %v", err)
		}
		if exists {
			t.Error("Expected share 100 to be removed")
		}
	})

	// Test GetShare for non-existent share
	t.Run("GetShare_NonExistent", func(t *testing.T) {
		_, err := tower.GetShare(key, 200)
		if err == nil {
			t.Error("Expected error when getting non-existent share")
		}
	})

	// Test RemoveShare for non-existent share
	t.Run("RemoveShare_NonExistent", func(t *testing.T) {
		err := tower.RemoveShare(key, 200)
		if err == nil {
			t.Error("Expected error when removing non-existent share")
		}
	})
}

func TestShamirListShareIDs(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test:shamir:listids"
	secret := []byte("Secret for listing share IDs")

	// Split secret
	shares, err := tower.SplitSecret(key, secret, 4, 2)
	if err != nil {
		t.Fatalf("Failed to split secret: %v", err)
	}

	// Get list of share IDs
	shareIDs, err := tower.ListShareIDs(key)
	if err != nil {
		t.Fatalf("Failed to list share IDs: %v", err)
	}

	if len(shareIDs) != 4 {
		t.Errorf("Expected 4 share IDs, got %d", len(shareIDs))
	}

	// Verify all original share IDs are present
	originalIDs := make(map[byte]bool)
	for id := range shares {
		originalIDs[id] = true
	}

	for _, id := range shareIDs {
		if !originalIDs[id] {
			t.Errorf("Unexpected share ID %d in list", id)
		}
		delete(originalIDs, id)
	}

	if len(originalIDs) > 0 {
		t.Errorf("Missing share IDs in list: %v", originalIDs)
	}
}

func TestShamirCombineSharesFrom(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	secret := []byte("Secret for external combination testing")

	// Split secret without storing (use Shamir library directly for test)
	shares, err := tower.SplitSecret("temp", secret, 5, 3)
	if err != nil {
		t.Fatalf("Failed to split secret: %v", err)
	}

	// Test combining from external shares
	t.Run("CombineSharesFrom_Valid", func(t *testing.T) {
		// Take first 3 shares
		selectedShares := make(map[byte][]byte)
		count := 0
		for id, share := range shares {
			if count >= 3 {
				break
			}
			selectedShares[id] = share
			count++
		}

		reconstructed, err := tower.CombineSharesFrom(selectedShares)
		if err != nil {
			t.Fatalf("Failed to combine external shares: %v", err)
		}

		if !bytes.Equal(reconstructed, secret) {
			t.Errorf("External combination failed. Expected: %s, Got: %s", string(secret), string(reconstructed))
		}
	})

	// Test with nil shares
	t.Run("CombineSharesFrom_Nil", func(t *testing.T) {
		_, err := tower.CombineSharesFrom(nil)
		if err == nil {
			t.Error("Expected error when combining nil shares")
		}
	})

	// Test with empty shares
	t.Run("CombineSharesFrom_Empty", func(t *testing.T) {
		_, err := tower.CombineSharesFrom(make(map[byte][]byte))
		if err == nil {
			t.Error("Expected error when combining empty shares")
		}
	})
}

func TestShamirErrorCases(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	// Test operations on non-existent key
	t.Run("NonExistentKey", func(t *testing.T) {
		nonExistentKey := "test:shamir:nonexistent"

		_, err := tower.GetShamirShare(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting shares from non-existent key")
		}

		_, err = tower.CombineShares(nonExistentKey)
		if err == nil {
			t.Error("Expected error when combining shares from non-existent key")
		}

		_, err = tower.GetShareCount(nonExistentKey)
		if err == nil {
			t.Error("Expected error when getting share count from non-existent key")
		}
	})

	// Test SetShamirShare with nil shares
	t.Run("SetShamirShare_Nil", func(t *testing.T) {
		key := "test:shamir:nil"
		err := tower.SetShamirShare(key, nil)
		if err == nil {
			t.Error("Expected error when setting nil shares")
		}
	})

	// Test invalid split parameters
	t.Run("SplitSecret_InvalidParams", func(t *testing.T) {
		key := "test:shamir:invalid"
		secret := []byte("test secret")

		// Test n < t (invalid threshold)
		_, err := tower.SplitSecret(key, secret, 2, 3)
		if err == nil {
			t.Error("Expected error when t > n in secret splitting")
		}
	})
}

func TestShamirConcurrentAccess(t *testing.T) {
	tower := createTestTower(t)
	defer tower.Close()

	key := "test:shamir:concurrent"
	secret := []byte("Secret for concurrent access testing")

	// Split secret initially
	_, err := tower.SplitSecret(key, secret, 5, 3)
	if err != nil {
		t.Fatalf("Failed to split secret: %v", err)
	}

	// Test concurrent reads
	t.Run("ConcurrentReads", func(t *testing.T) {
		done := make(chan bool, 10)

		for i := 0; i < 10; i++ {
			go func() {
				defer func() { done <- true }()

				// Read shares
				shares, err := tower.GetShamirShare(key)
				if err != nil {
					t.Errorf("Failed to get shares in concurrent read: %v", err)
					return
				}

				if len(shares) != 5 {
					t.Errorf("Expected 5 shares in concurrent read, got %d", len(shares))
					return
				}

				// Combine shares
				reconstructed, err := tower.CombineShares(key)
				if err != nil {
					t.Errorf("Failed to combine shares in concurrent read: %v", err)
					return
				}

				if !bytes.Equal(reconstructed, secret) {
					t.Errorf("Secret mismatch in concurrent read")
				}
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < 10; i++ {
			<-done
		}
	})
}
