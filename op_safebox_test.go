package tower

import (
	"bytes"
	"fmt"
	"testing"
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

// Note: Tower methods (UpsertSafeBox, GetSafeBox, ExtractSafeBox) require a Tower instance and database setup,
// which might be tested separately in integration tests.
