package tower

import (
	"bytes"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/scrypt"
)

type PasswordAlgorithm uint16

const (
	PasswordAlgorithmBcrypt PasswordAlgorithm = iota + 1
	PasswordAlgorithmArgon2i
	PasswordAlgorithmArgon2id
	PasswordAlgorithmScrypt
	PasswordAlgorithmPBKDF2
)

const (
	DefaultPasswordSaltLength = 16
)

func (t *Tower) UpsertPassword(key string, password []byte, algorithm PasswordAlgorithm, saltLength int) error {
	unlock := t.lock(key)
	defer unlock()

	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}

	hashed, err := []byte(nil), error(nil)
	switch algorithm {
	case PasswordAlgorithmBcrypt:
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		hashed, err = bcrypt.GenerateFromPassword(salted, bcrypt.DefaultCost)
	case PasswordAlgorithmScrypt:
		const (
			N      = 16384
			r      = 8
			p      = 1
			keyLen = 32
		)
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		hashed, err = scrypt.Key(salted, salt, N, r, p, keyLen)
	case PasswordAlgorithmPBKDF2:
		const (
			iterations = 10000
			keyLen     = 32
		)
		hashed, err = pbkdf2.Key(sha256.New, string(password), salt, iterations, keyLen)
	case PasswordAlgorithmArgon2i:
		const (
			time    = 3
			memory  = 32 * 1024
			threads = 4
			keyLen  = 32
		)
		hashed = argon2.Key(password, salt, time, memory, threads, keyLen)
	case PasswordAlgorithmArgon2id:
		fallthrough
	default:
		const (
			time    = 3
			memory  = 32 * 1024
			threads = 4
			keyLen  = 32
		)
		hashed = argon2.IDKey(password, salt, time, memory, threads, keyLen)
	}
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetPassword(algorithm, hashed, salt); err != nil {
		return fmt.Errorf("failed to set password data: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set password data: %w", err)
	}

	return nil
}

func (t *Tower) VerifyPassword(key string, password []byte) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	if df.typ != TypePassword {
		return false, fmt.Errorf("key %s is not a password type", key)
	}

	algorithm, hash, salt, err := df.Password()
	if err != nil {
		return false, fmt.Errorf("failed to get password data: %w", err)
	}

	switch algorithm {
	case PasswordAlgorithmBcrypt:
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		err = bcrypt.CompareHashAndPassword(hash, salted)
		if err != nil {
			if err == bcrypt.ErrMismatchedHashAndPassword {
				return false, nil
			}
			return false, fmt.Errorf("failed to compare bcrypt password: %w", err)
		}
	case PasswordAlgorithmScrypt:
		const (
			N      = 16384
			r      = 8
			p      = 1
			keyLen = 32
		)
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		computed, err := scrypt.Key(salted, salt, N, r, p, keyLen)
		if err != nil {
			return false, fmt.Errorf("failed to compute scrypt key: %w", err)
		}
		if !bytes.Equal(computed, hash) {
			return false, nil
		}
	case PasswordAlgorithmPBKDF2:
		const (
			iterations = 10000
			keyLen     = 32
		)
		computed, err := pbkdf2.Key(sha256.New, string(password), salt, iterations, keyLen)
		if err != nil {
			return false, fmt.Errorf("failed to compute pbkdf2 key: %w", err)
		}
		if !bytes.Equal(computed, hash) {
			return false, nil
		}
	case PasswordAlgorithmArgon2i:
		const (
			time    = 3
			memory  = 32 * 1024
			threads = 4
			keyLen  = 32
		)
		computed := argon2.Key(password, salt, time, memory, threads, keyLen)
		if !bytes.Equal(computed, hash) {
			return false, nil
		}
	case PasswordAlgorithmArgon2id:
		const (
			time    = 3
			memory  = 32 * 1024
			threads = 4
			keyLen  = 32
		)
		computed := argon2.IDKey(password, salt, time, memory, threads, keyLen)
		if !bytes.Equal(computed, hash) {
			return false, nil
		}
	default:
		return false, fmt.Errorf("unknown password algorithm: %d", algorithm)
	}

	return true, nil
}
