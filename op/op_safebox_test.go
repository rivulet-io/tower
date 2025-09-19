package op

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestGetNonce(t *testing.T) {
	length := 12
	nonce, err := getNonce(length)
	if err != nil {
		t.Fatalf("Failed to generate nonce: %v", err)
	}
	if len(nonce) != length {
		t.Errorf("Expected nonce length %d, got %d", length, len(nonce))
	}
}

func TestGetAEAD(t *testing.T) {
	key := []byte("test-key-12345678901234567890123456789012") // 32 bytes

	algorithms := []EncryptionAlgorithm{
		EncryptionAlgorithmAES128GCM,
		EncryptionAlgorithmAES192GCM,
		EncryptionAlgorithmAES256GCM,
		EncryptionAlgorithmChaCha20Poly1305,
		EncryptionAlgorithmXChaCha20Poly1305,
		EncryptionAlgorithmAscon128,
		EncryptionAlgorithmAscon128a,
		EncryptionAlgorithmAscon80pq,
		EncryptionAlgorithmCamellia128GCM,
		EncryptionAlgorithmCamellia192GCM,
		EncryptionAlgorithmCamellia256GCM,
		EncryptionAlgorithmARIA128GCM,
		EncryptionAlgorithmARIA192GCM,
		EncryptionAlgorithmARIA256GCM,
	}

	for _, algo := range algorithms {
		t.Run(fmt.Sprintf("%d", algo), func(t *testing.T) {
			aead, err := getAEAD(algo, key)
			if err != nil {
				t.Errorf("Failed to get AEAD for %d: %v", algo, err)
			}
			if aead == nil {
				t.Errorf("AEAD is nil for %d", algo)
			}
		})
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	plainText := []byte("Hello, World! This is a test message.")
	key := []byte("test-key-12345678901234567890123456789012") // 32 bytes

	algorithms := []EncryptionAlgorithm{
		EncryptionAlgorithmNone,
		EncryptionAlgorithmAES128GCM,
		EncryptionAlgorithmAES192GCM,
		EncryptionAlgorithmAES256GCM,
		EncryptionAlgorithmChaCha20Poly1305,
		EncryptionAlgorithmXChaCha20Poly1305,
		EncryptionAlgorithmAscon128,
		EncryptionAlgorithmAscon128a,
		EncryptionAlgorithmAscon80pq,
		EncryptionAlgorithmCamellia128GCM,
		EncryptionAlgorithmCamellia192GCM,
		EncryptionAlgorithmCamellia256GCM,
		EncryptionAlgorithmARIA128GCM,
		EncryptionAlgorithmARIA192GCM,
		EncryptionAlgorithmARIA256GCM,
	}

	for _, algo := range algorithms {
		t.Run(fmt.Sprintf("%d", algo), func(t *testing.T) {
			encryptedData, nonce, err := encryptData(plainText, key, algo)
			if err != nil {
				t.Fatalf("Failed to encrypt data: %v", err)
			}

			decryptedData, err := decryptData(encryptedData, nonce, key, algo)
			if err != nil {
				t.Fatalf("Failed to decrypt data: %v", err)
			}

			if !bytes.Equal(plainText, decryptedData) {
				t.Errorf("Decrypted data does not match original. Original: %s, Decrypted: %s", plainText, decryptedData)
			}
		})
	}
}

func TestSafeBoxOperations(t *testing.T) {
	tower, err := NewOperator(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test basic SafeBox operations with AES256GCM
	t.Run("basic safebox operations", func(t *testing.T) {
		key := "test_safebox"
		data := []byte("Secret message for testing SafeBox functionality")
		encKey := []byte("my-encryption-key-32-bytes-long!")

		// Test UpsertSafeBox
		payload, err := tower.UpsertSafeBox(key, data, encKey, EncryptionAlgorithmAES256GCM)
		if err != nil {
			t.Errorf("UpsertSafeBox failed: %v", err)
		}
		if len(payload) == 0 {
			t.Error("Expected non-empty payload")
		}

		// Test GetSafeBox
		algorithm, encryptedData, nonce, err := tower.GetSafeBox(key)
		if err != nil {
			t.Errorf("GetSafeBox failed: %v", err)
		}
		if algorithm != EncryptionAlgorithmAES256GCM {
			t.Errorf("Expected algorithm %d, got %d", EncryptionAlgorithmAES256GCM, algorithm)
		}
		if len(encryptedData) == 0 {
			t.Error("Expected non-empty encrypted data")
		}
		if len(nonce) == 0 {
			t.Error("Expected non-empty nonce")
		}

		// Test ExtractSafeBox
		decryptedData, err := tower.ExtractSafeBox(key, encKey)
		if err != nil {
			t.Errorf("ExtractSafeBox failed: %v", err)
		}
		if !bytes.Equal(data, decryptedData) {
			t.Errorf("Decrypted data does not match original. Original: %s, Decrypted: %s", data, decryptedData)
		}
	})

	// Test SafeBox with different encryption algorithms
	t.Run("multiple encryption algorithms", func(t *testing.T) {
		data := []byte("Test data for multiple algorithms")
		encKey := []byte("test-key-for-multiple-algorithms")

		algorithms := []EncryptionAlgorithm{
			EncryptionAlgorithmNone,
			EncryptionAlgorithmAES128GCM,
			EncryptionAlgorithmAES192GCM,
			EncryptionAlgorithmAES256GCM,
			EncryptionAlgorithmChaCha20Poly1305,
			EncryptionAlgorithmXChaCha20Poly1305,
			EncryptionAlgorithmAscon128,
			EncryptionAlgorithmAscon128a,
			EncryptionAlgorithmAscon80pq,
			EncryptionAlgorithmCamellia128GCM,
			EncryptionAlgorithmCamellia192GCM,
			EncryptionAlgorithmCamellia256GCM,
			EncryptionAlgorithmARIA128GCM,
			EncryptionAlgorithmARIA192GCM,
			EncryptionAlgorithmARIA256GCM,
		}

		for _, algo := range algorithms {
			t.Run(fmt.Sprintf("algorithm_%d", algo), func(t *testing.T) {
				key := fmt.Sprintf("test_safebox_%d", algo)

				// Store data with algorithm
				_, err := tower.UpsertSafeBox(key, data, encKey, algo)
				if err != nil {
					t.Errorf("UpsertSafeBox failed for algorithm %d: %v", algo, err)
				}

				// Retrieve and verify algorithm
				retrievedAlgo, _, _, err := tower.GetSafeBox(key)
				if err != nil {
					t.Errorf("GetSafeBox failed for algorithm %d: %v", algo, err)
				}
				if retrievedAlgo != algo {
					t.Errorf("Algorithm mismatch: expected %d, got %d", algo, retrievedAlgo)
				}

				// Extract and verify data
				decryptedData, err := tower.ExtractSafeBox(key, encKey)
				if err != nil {
					t.Errorf("ExtractSafeBox failed for algorithm %d: %v", algo, err)
				}
				if !bytes.Equal(data, decryptedData) {
					t.Errorf("Data mismatch for algorithm %d", algo)
				}
			})
		}
	})

	// Test SafeBox with wrong encryption key
	t.Run("wrong encryption key", func(t *testing.T) {
		key := "test_wrong_key"
		data := []byte("Secret data")
		correctKey := []byte("correct-key")
		wrongKey := []byte("wrong-key")

		// Store with correct key
		_, err := tower.UpsertSafeBox(key, data, correctKey, EncryptionAlgorithmAES256GCM)
		if err != nil {
			t.Errorf("UpsertSafeBox failed: %v", err)
		}

		// Try to extract with wrong key
		_, err = tower.ExtractSafeBox(key, wrongKey)
		if err == nil {
			t.Error("Expected error when using wrong encryption key")
		}

		// Verify correct key still works
		decryptedData, err := tower.ExtractSafeBox(key, correctKey)
		if err != nil {
			t.Errorf("ExtractSafeBox failed with correct key: %v", err)
		}
		if !bytes.Equal(data, decryptedData) {
			t.Error("Data does not match when using correct key")
		}
	})

	// Test SafeBox with empty data
	t.Run("empty data", func(t *testing.T) {
		key := "test_empty"
		data := []byte("")
		encKey := []byte("key-for-empty-data")

		_, err := tower.UpsertSafeBox(key, data, encKey, EncryptionAlgorithmAES256GCM)
		if err != nil {
			t.Errorf("UpsertSafeBox failed with empty data: %v", err)
		}

		decryptedData, err := tower.ExtractSafeBox(key, encKey)
		if err != nil {
			t.Errorf("ExtractSafeBox failed with empty data: %v", err)
		}
		if !bytes.Equal(data, decryptedData) {
			t.Error("Empty data does not match")
		}
	})

	// Test SafeBox overwrite
	t.Run("overwrite safebox", func(t *testing.T) {
		key := "test_overwrite"
		data1 := []byte("First secret message")
		data2 := []byte("Second secret message")
		encKey := []byte("overwrite-test-key")

		// Store first data
		_, err := tower.UpsertSafeBox(key, data1, encKey, EncryptionAlgorithmAES256GCM)
		if err != nil {
			t.Errorf("First UpsertSafeBox failed: %v", err)
		}

		// Verify first data
		decryptedData, err := tower.ExtractSafeBox(key, encKey)
		if err != nil {
			t.Errorf("First ExtractSafeBox failed: %v", err)
		}
		if !bytes.Equal(data1, decryptedData) {
			t.Error("First data does not match")
		}

		// Overwrite with second data
		_, err = tower.UpsertSafeBox(key, data2, encKey, EncryptionAlgorithmChaCha20Poly1305)
		if err != nil {
			t.Errorf("Second UpsertSafeBox failed: %v", err)
		}

		// Verify algorithm changed
		algorithm, _, _, err := tower.GetSafeBox(key)
		if err != nil {
			t.Errorf("GetSafeBox after overwrite failed: %v", err)
		}
		if algorithm != EncryptionAlgorithmChaCha20Poly1305 {
			t.Errorf("Expected algorithm %d, got %d", EncryptionAlgorithmChaCha20Poly1305, algorithm)
		}

		// Verify second data
		decryptedData, err = tower.ExtractSafeBox(key, encKey)
		if err != nil {
			t.Errorf("Second ExtractSafeBox failed: %v", err)
		}
		if !bytes.Equal(data2, decryptedData) {
			t.Error("Second data does not match")
		}
		if bytes.Equal(data1, decryptedData) {
			t.Error("Old data should have been overwritten")
		}
	})

	// Test SafeBox with large data
	t.Run("large data", func(t *testing.T) {
		key := "test_large"
		// Create 1MB of test data
		data := make([]byte, 1024*1024)
		for i := range data {
			data[i] = byte(i % 256)
		}
		encKey := []byte("large-data-test-key")

		_, err := tower.UpsertSafeBox(key, data, encKey, EncryptionAlgorithmAES256GCM)
		if err != nil {
			t.Errorf("UpsertSafeBox failed with large data: %v", err)
		}

		decryptedData, err := tower.ExtractSafeBox(key, encKey)
		if err != nil {
			t.Errorf("ExtractSafeBox failed with large data: %v", err)
		}
		if !bytes.Equal(data, decryptedData) {
			t.Error("Large data does not match")
		}
	})

	// Test SafeBox with non-existent key
	t.Run("non-existent key", func(t *testing.T) {
		key := "non_existent_key"
		encKey := []byte("any-key")

		_, _, _, err := tower.GetSafeBox(key)
		if err == nil {
			t.Error("Expected error when getting non-existent SafeBox")
		}

		_, err = tower.ExtractSafeBox(key, encKey)
		if err == nil {
			t.Error("Expected error when extracting non-existent SafeBox")
		}
	})
}
