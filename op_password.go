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

type PasswordOptions struct {
	// Bcrypt options
	BcryptCost int `json:"bcrypt_cost,omitempty,omitzero"`

	// Scrypt options
	ScryptN      int `json:"scrypt_n,omitempty,omitzero"`
	ScryptR      int `json:"scrypt_r,omitempty,omitzero"`
	ScryptP      int `json:"scrypt_p,omitempty,omitzero"`
	ScryptKeyLen int `json:"scrypt_key_len,omitempty,omitzero"`

	// PBKDF2 options
	PBKDF2Iterations int `json:"pbkdf2_iterations,omitempty,omitzero"`
	PBKDF2KeyLen     int `json:"pbkdf2_key_len,omitempty,omitzero"`

	// Argon2 options
	Argon2Time    uint32 `json:"argon2_time,omitempty,omitzero"`
	Argon2Memory  uint32 `json:"argon2_memory,omitempty,omitzero"`
	Argon2Threads uint8  `json:"argon2_threads,omitempty,omitzero"`
	Argon2KeyLen  uint32 `json:"argon2_key_len,omitempty,omitzero"`
}

// 각 알고리즘별 기본값 생성자들
func DefaultBcryptOptions() *PasswordOptions {
	return &PasswordOptions{
		BcryptCost: bcrypt.DefaultCost,
	}
}

func DefaultScryptOptions() *PasswordOptions {
	return &PasswordOptions{
		ScryptN:      16384,
		ScryptR:      8,
		ScryptP:      1,
		ScryptKeyLen: 32,
	}
}

func DefaultPBKDF2Options() *PasswordOptions {
	return &PasswordOptions{
		PBKDF2Iterations: 10000,
		PBKDF2KeyLen:     32,
	}
}

func DefaultArgon2Options() *PasswordOptions {
	return &PasswordOptions{
		Argon2Time:    3,
		Argon2Memory:  32 * 1024,
		Argon2Threads: 4,
		Argon2KeyLen:  32,
	}
}

// 알고리즘에 따른 기본 옵션 반환
func DefaultPasswordOptions(algorithm PasswordAlgorithm) *PasswordOptions {
	switch algorithm {
	case PasswordAlgorithmBcrypt:
		return DefaultBcryptOptions()
	case PasswordAlgorithmScrypt:
		return DefaultScryptOptions()
	case PasswordAlgorithmPBKDF2:
		return DefaultPBKDF2Options()
	case PasswordAlgorithmArgon2i, PasswordAlgorithmArgon2id:
		return DefaultArgon2Options()
	default:
		return DefaultArgon2Options()
	}
}

// 함수형 옵션 패턴
type PasswordOption func(*PasswordOptions)

func WithBcryptCost(cost int) PasswordOption {
	return func(opts *PasswordOptions) {
		opts.BcryptCost = cost
	}
}

func WithScryptParams(N, r, p, keyLen int) PasswordOption {
	return func(opts *PasswordOptions) {
		opts.ScryptN = N
		opts.ScryptR = r
		opts.ScryptP = p
		opts.ScryptKeyLen = keyLen
	}
}

func WithPBKDF2Params(iterations, keyLen int) PasswordOption {
	return func(opts *PasswordOptions) {
		opts.PBKDF2Iterations = iterations
		opts.PBKDF2KeyLen = keyLen
	}
}

func WithArgon2Params(time, memory uint32, threads uint8, keyLen uint32) PasswordOption {
	return func(opts *PasswordOptions) {
		opts.Argon2Time = time
		opts.Argon2Memory = memory
		opts.Argon2Threads = threads
		opts.Argon2KeyLen = keyLen
	}
}

func (t *Tower) UpsertPassword(key string, password []byte, algorithm PasswordAlgorithm, saltLength int, options ...PasswordOption) error {
	opts := DefaultPasswordOptions(algorithm)
	for _, option := range options {
		option(opts)
	}

	unlock := t.lock(key)
	defer unlock()

	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("failed to generate salt: %w", err)
	}

	hashed, err := t.computePasswordHash(password, salt, algorithm, opts)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetPasswordWithOptions(algorithm, hashed, salt, opts); err != nil {
		return fmt.Errorf("failed to set password data: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set password data: %w", err)
	}

	return nil
}

// 통합된 패스워드 해시 계산 함수
func (t *Tower) computePasswordHash(password, salt []byte, algorithm PasswordAlgorithm, opts *PasswordOptions) ([]byte, error) {
	switch algorithm {
	case PasswordAlgorithmBcrypt:
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		return bcrypt.GenerateFromPassword(salted, opts.BcryptCost)
	case PasswordAlgorithmScrypt:
		salted := make([]byte, len(password)+len(salt)*2)
		copy(salted, salt)
		copy(salted[len(salt):], password)
		copy(salted[len(salt)+len(password):], salt)
		return scrypt.Key(salted, salt, opts.ScryptN, opts.ScryptR, opts.ScryptP, opts.ScryptKeyLen)
	case PasswordAlgorithmPBKDF2:
		return pbkdf2.Key(sha256.New, string(password), salt, opts.PBKDF2Iterations, opts.PBKDF2KeyLen)
	case PasswordAlgorithmArgon2i:
		return argon2.Key(password, salt, opts.Argon2Time, opts.Argon2Memory, opts.Argon2Threads, opts.Argon2KeyLen), nil
	case PasswordAlgorithmArgon2id:
		fallthrough
	default:
		return argon2.IDKey(password, salt, opts.Argon2Time, opts.Argon2Memory, opts.Argon2Threads, opts.Argon2KeyLen), nil
	}
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

	algorithm, hash, salt, opts, err := df.Password()
	if err != nil {
		return false, fmt.Errorf("failed to get password data: %w", err)
	}

	switch algorithm {
	case PasswordAlgorithmBcrypt:
		// Bcrypt는 특별 처리 (내부적으로 이미 해시된 값과 비교)
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
		return true, nil
	default:
		// 다른 알고리즘들은 통합 헬퍼 함수 사용
		computed, err := t.computePasswordHash(password, salt, algorithm, opts)
		if err != nil {
			return false, fmt.Errorf("failed to compute password hash: %w", err)
		}
		return bytes.Equal(computed, hash), nil
	}
}
