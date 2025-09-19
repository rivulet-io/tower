package tower

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	"github.com/RyuaNerin/go-krypto/aria"
	"github.com/dgryski/go-camellia"
	"github.com/linckode/circl/cipher/ascon"
	"golang.org/x/crypto/chacha20poly1305"
	"lukechampine.com/blake3"
)

type EncryptionAlgorithm uint16

const (
	EncryptionAlgorithmNone EncryptionAlgorithm = iota
	EncryptionAlgorithmAES128GCM
	EncryptionAlgorithmAES256GCM
	EncryptionAlgorithmAES512GCM
	EncryptionAlgorithmChaCha20Poly1305
	EncryptionAlgorithmXChaCha20Poly1305
	EncryptionAlgorithmAscon128
	EncryptionAlgorithmAscon128a
	EncryptionAlgorithmAscon80pq
	EncryptionAlgorithmCamellia128GCM
	EncryptionAlgorithmCamellia192GCM
	EncryptionAlgorithmCamellia256GCM
	EncryptionAlgorithmARIA128GCM
	EncryptionAlgorithmARIA192GCM
	EncryptionAlgorithmARIA256GCM
)

func getAEAD(algorithm EncryptionAlgorithm, key []byte) (cipher.AEAD, error) {
	hashedKey := func(length int) ([]byte, error) {
		hasher := blake3.New(length, nil)
		_, err := hasher.Write(key)
		if err != nil {
			return nil, fmt.Errorf("failed to hash key: %w", err)
		}

		return hasher.Sum(nil), nil
	}

	switch algorithm {
	case EncryptionAlgorithmAES128GCM:
		hashedKey, err := hashedKey(16)
		if err != nil {
			return nil, err
		}
		block, err := aes.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmAES256GCM:
		hashedKey, err := hashedKey(32)
		if err != nil {
			return nil, err
		}
		block, err := aes.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmAES512GCM:
		hashedKey, err := hashedKey(64)
		if err != nil {
			return nil, err
		}
		block, err := aes.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create AES cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmChaCha20Poly1305:
		hashedKey, err := hashedKey(32)
		if err != nil {
			return nil, err
		}
		aead, err := chacha20poly1305.New(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create ChaCha20-Poly1305: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmXChaCha20Poly1305:
		hashedKey, err := hashedKey(32)
		if err != nil {
			return nil, err
		}
		aead, err := chacha20poly1305.NewX(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create XChaCha20-Poly1305: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmAscon128:
		hashedKey, err := hashedKey(ascon.Ascon128.KeySize())
		if err != nil {
			return nil, err
		}
		aead, err := ascon.New(hashedKey, ascon.Ascon128)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ascon128 cipher: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmAscon128a:
		hashedKey, err := hashedKey(ascon.Ascon128a.KeySize())
		if err != nil {
			return nil, err
		}
		aead, err := ascon.New(hashedKey, ascon.Ascon128a)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ascon128a cipher: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmAscon80pq:
		hashedKey, err := hashedKey(ascon.Ascon80pq.KeySize())
		if err != nil {
			return nil, err
		}
		aead, err := ascon.New(hashedKey, ascon.Ascon80pq)
		if err != nil {
			return nil, fmt.Errorf("failed to create Ascon80pq cipher: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmCamellia128GCM:
		hashedKey, err := hashedKey(16)
		if err != nil {
			return nil, err
		}
		block, err := camellia.New(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Camellia cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmCamellia192GCM:
		hashedKey, err := hashedKey(24)
		if err != nil {
			return nil, err
		}
		block, err := camellia.New(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Camellia cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmCamellia256GCM:
		hashedKey, err := hashedKey(32)
		if err != nil {
			return nil, err
		}
		block, err := camellia.New(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Camellia cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmARIA128GCM:
		hashedKey, err := hashedKey(16)
		if err != nil {
			return nil, err
		}
		block, err := aria.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARIA cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmARIA192GCM:
		hashedKey, err := hashedKey(24)
		if err != nil {
			return nil, err
		}
		block, err := aria.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARIA cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	case EncryptionAlgorithmARIA256GCM:
		hashedKey, err := hashedKey(32)
		if err != nil {
			return nil, err
		}
		block, err := aria.NewCipher(hashedKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create ARIA cipher: %w", err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCM: %w", err)
		}
		return aead, nil
	default:
		return nil, fmt.Errorf("unsupported encryption algorithm: %d", algorithm)
	}
}

func getNonce(length int) ([]byte, error) {
	nonce := make([]byte, length)
	_, err := rand.Read(nonce)
	if err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	return nonce, nil
}

func encryptData(plainText, key []byte, algorithm EncryptionAlgorithm) ([]byte, []byte, error) {
	var encryptedData, nonce []byte
	var err error

	switch algorithm {
	case EncryptionAlgorithmNone:
		encryptedData = plainText
	default:
		var aead cipher.AEAD
		aead, err = getAEAD(algorithm, key)
		if err != nil {
			return nil, nil, err
		}
		nonce, err = getNonce(aead.NonceSize())
		if err != nil {
			return nil, nil, err
		}
		encryptedData = aead.Seal(nil, nonce, plainText, nil)
	}

	return encryptedData, nonce, nil
}

func (t *Tower) UpsertSafeBox(key string, data []byte, encKey []byte, algorithm EncryptionAlgorithm) ([]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	encryptedData, nonce, err := encryptData(data, encKey, algorithm)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt data: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetSafeBox(algorithm, encryptedData, nonce); err != nil {
		return nil, fmt.Errorf("failed to set safebox data frame: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to upsert safebox: %w", err)
	}

	payload := make([]byte, len(df.payload))
	copy(payload, df.payload)

	return payload, nil
}

func (t *Tower) GetSafeBox(key string) (EncryptionAlgorithm, []byte, []byte, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to get safebox: %w", err)
	}

	algorithm, encryptedData, nonce, err := df.SafeBox()
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to get safebox data: %w", err)
	}

	return algorithm, encryptedData, nonce, nil
}

func decryptData(encryptedData, nonce, key []byte, algorithm EncryptionAlgorithm) ([]byte, error) {
	if algorithm == EncryptionAlgorithmNone {
		return encryptedData, nil
	}

	aead, err := getAEAD(algorithm, key)
	if err != nil {
		return nil, err
	}

	plainText, err := aead.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	return plainText, nil
}

func (t *Tower) ExtractSafeBox(key string, encKey []byte) ([]byte, error) {
	algorithm, encryptedData, nonce, err := t.GetSafeBox(key)
	if err != nil {
		return nil, err
	}

	plainText, err := decryptData(encryptedData, nonce, encKey, algorithm)
	if err != nil {
		return nil, err
	}

	return plainText, nil
}
