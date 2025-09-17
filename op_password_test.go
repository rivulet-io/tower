package tower

import (
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestPasswordOperations(t *testing.T) {
	tower, err := NewTower(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    NewSizeFromMegabytes(64),
		MemTableSize: NewSizeFromMegabytes(16),
		BytesPerSync: NewSizeFromKilobytes(512),
	})
	if err != nil {
		t.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	// Test UpsertPassword and VerifyPassword with different algorithms
	t.Run("bcrypt password operations", func(t *testing.T) {
		key := "test_user_bcrypt"
		password := []byte("mySecretPassword123!")

		// Test upsert with default options
		err := tower.UpsertPassword(key, password, PasswordAlgorithmBcrypt, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Test verify correct password
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}

		// Test verify incorrect password
		isValid, err = tower.VerifyPassword(key, []byte("wrongPassword"))
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected password to be invalid")
		}
	})

	t.Run("bcrypt password with custom options", func(t *testing.T) {
		key := "test_user_bcrypt_custom"
		password := []byte("customPassword456!")

		// Test upsert with custom bcrypt cost
		err := tower.UpsertPassword(key, password, PasswordAlgorithmBcrypt, DefaultPasswordSaltLength,
			WithBcryptCost(10))
		if err != nil {
			t.Errorf("UpsertPassword with custom options failed: %v", err)
		}

		// Test verify
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}
	})

	t.Run("scrypt password operations", func(t *testing.T) {
		key := "test_user_scrypt"
		password := []byte("scryptPassword789!")

		// Test upsert with default options
		err := tower.UpsertPassword(key, password, PasswordAlgorithmScrypt, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Test verify correct password
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}

		// Test verify incorrect password
		isValid, err = tower.VerifyPassword(key, []byte("wrongPassword"))
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected password to be invalid")
		}
	})

	t.Run("scrypt password with custom options", func(t *testing.T) {
		key := "test_user_scrypt_custom"
		password := []byte("customScryptPassword!")

		// Test upsert with custom scrypt parameters
		err := tower.UpsertPassword(key, password, PasswordAlgorithmScrypt, DefaultPasswordSaltLength,
			WithScryptParams(8192, 8, 1, 32))
		if err != nil {
			t.Errorf("UpsertPassword with custom options failed: %v", err)
		}

		// Test verify
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}
	})

	t.Run("pbkdf2 password operations", func(t *testing.T) {
		key := "test_user_pbkdf2"
		password := []byte("pbkdf2Password!")

		// Test upsert with default options
		err := tower.UpsertPassword(key, password, PasswordAlgorithmPBKDF2, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Test verify correct password
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}

		// Test verify incorrect password
		isValid, err = tower.VerifyPassword(key, []byte("wrongPassword"))
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected password to be invalid")
		}
	})

	t.Run("pbkdf2 password with custom options", func(t *testing.T) {
		key := "test_user_pbkdf2_custom"
		password := []byte("customPBKDF2Password!")

		// Test upsert with custom PBKDF2 parameters
		err := tower.UpsertPassword(key, password, PasswordAlgorithmPBKDF2, DefaultPasswordSaltLength,
			WithPBKDF2Params(20000, 32))
		if err != nil {
			t.Errorf("UpsertPassword with custom options failed: %v", err)
		}

		// Test verify
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}
	})

	t.Run("argon2i password operations", func(t *testing.T) {
		key := "test_user_argon2i"
		password := []byte("argon2iPassword!")

		// Test upsert with default options
		err := tower.UpsertPassword(key, password, PasswordAlgorithmArgon2i, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Test verify correct password
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}

		// Test verify incorrect password
		isValid, err = tower.VerifyPassword(key, []byte("wrongPassword"))
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected password to be invalid")
		}
	})

	t.Run("argon2id password operations", func(t *testing.T) {
		key := "test_user_argon2id"
		password := []byte("argon2idPassword!")

		// Test upsert with default options
		err := tower.UpsertPassword(key, password, PasswordAlgorithmArgon2id, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Test verify correct password
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}

		// Test verify incorrect password
		isValid, err = tower.VerifyPassword(key, []byte("wrongPassword"))
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected password to be invalid")
		}
	})

	t.Run("argon2id password with custom options", func(t *testing.T) {
		key := "test_user_argon2id_custom"
		password := []byte("customArgon2idPassword!")

		// Test upsert with custom Argon2 parameters
		err := tower.UpsertPassword(key, password, PasswordAlgorithmArgon2id, DefaultPasswordSaltLength,
			WithArgon2Params(5, 64*1024, 8, 32))
		if err != nil {
			t.Errorf("UpsertPassword with custom options failed: %v", err)
		}

		// Test verify
		isValid, err := tower.VerifyPassword(key, password)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected password to be valid")
		}
	})

	// Test error cases
	t.Run("password error cases", func(t *testing.T) {
		// Test verifying non-existent key
		_, err := tower.VerifyPassword("non_existent_key", []byte("password"))
		if err == nil {
			t.Error("Expected error for non-existent key")
		}

		// Test verifying wrong type key
		tower.SetString("string_key", "not_a_password")
		_, err = tower.VerifyPassword("string_key", []byte("password"))
		if err == nil {
			t.Error("Expected error for wrong type key")
		}
	})

	// Test default options functions
	t.Run("default options functions", func(t *testing.T) {
		bcryptOpts := DefaultBcryptOptions()
		if bcryptOpts.BcryptCost != bcrypt.DefaultCost {
			t.Errorf("Expected BcryptCost to be %d, got %d", bcrypt.DefaultCost, bcryptOpts.BcryptCost)
		}

		scryptOpts := DefaultScryptOptions()
		if scryptOpts.ScryptN != 16384 {
			t.Errorf("Expected ScryptN to be 16384, got %d", scryptOpts.ScryptN)
		}

		pbkdf2Opts := DefaultPBKDF2Options()
		if pbkdf2Opts.PBKDF2Iterations != 10000 {
			t.Errorf("Expected PBKDF2Iterations to be 10000, got %d", pbkdf2Opts.PBKDF2Iterations)
		}

		argon2Opts := DefaultArgon2Options()
		if argon2Opts.Argon2Time != 3 {
			t.Errorf("Expected Argon2Time to be 3, got %d", argon2Opts.Argon2Time)
		}

		// Test algorithm-specific defaults
		for _, algo := range []PasswordAlgorithm{
			PasswordAlgorithmBcrypt,
			PasswordAlgorithmScrypt,
			PasswordAlgorithmPBKDF2,
			PasswordAlgorithmArgon2i,
			PasswordAlgorithmArgon2id,
		} {
			opts := DefaultPasswordOptions(algo)
			if opts == nil {
				t.Errorf("DefaultPasswordOptions returned nil for algorithm %d", algo)
			}
		}
	})

	// Test password update (overwrite existing)
	t.Run("password update", func(t *testing.T) {
		key := "test_user_update"
		oldPassword := []byte("oldPassword123!")
		newPassword := []byte("newPassword456!")

		// Set initial password
		err := tower.UpsertPassword(key, oldPassword, PasswordAlgorithmArgon2id, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword failed: %v", err)
		}

		// Verify old password works
		isValid, err := tower.VerifyPassword(key, oldPassword)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected old password to be valid")
		}

		// Update password
		err = tower.UpsertPassword(key, newPassword, PasswordAlgorithmArgon2id, DefaultPasswordSaltLength)
		if err != nil {
			t.Errorf("UpsertPassword update failed: %v", err)
		}

		// Verify old password no longer works
		isValid, err = tower.VerifyPassword(key, oldPassword)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if isValid {
			t.Error("Expected old password to be invalid after update")
		}

		// Verify new password works
		isValid, err = tower.VerifyPassword(key, newPassword)
		if err != nil {
			t.Errorf("VerifyPassword failed: %v", err)
		}
		if !isValid {
			t.Error("Expected new password to be valid")
		}
	})

	// Test different salt lengths
	t.Run("different salt lengths", func(t *testing.T) {
		password := []byte("testPassword!")
		saltLengths := []int{8, 16, 32, 64}

		for _, saltLen := range saltLengths {
			key := "test_salt_" + string(rune(saltLen))

			err := tower.UpsertPassword(key, password, PasswordAlgorithmArgon2id, saltLen)
			if err != nil {
				t.Errorf("UpsertPassword with salt length %d failed: %v", saltLen, err)
			}

			isValid, err := tower.VerifyPassword(key, password)
			if err != nil {
				t.Errorf("VerifyPassword with salt length %d failed: %v", saltLen, err)
			}
			if !isValid {
				t.Errorf("Expected password to be valid with salt length %d", saltLen)
			}
		}
	})
}

// Benchmark tests
func BenchmarkPasswordOperations(b *testing.B) {
	tower, err := NewTower(&Options{
		Path:         "data",
		FS:           InMemory(),
		CacheSize:    NewSizeFromMegabytes(64),
		MemTableSize: NewSizeFromMegabytes(16),
		BytesPerSync: NewSizeFromKilobytes(512),
	})
	if err != nil {
		b.Fatalf("Failed to create tower: %v", err)
	}
	defer tower.Close()

	password := []byte("benchmarkPassword123!")

	b.Run("UpsertPassword-Bcrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench_bcrypt_" + string(rune(i))
			tower.UpsertPassword(key, password, PasswordAlgorithmBcrypt, DefaultPasswordSaltLength)
		}
	})

	b.Run("UpsertPassword-Scrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench_scrypt_" + string(rune(i))
			tower.UpsertPassword(key, password, PasswordAlgorithmScrypt, DefaultPasswordSaltLength)
		}
	})

	b.Run("UpsertPassword-PBKDF2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench_pbkdf2_" + string(rune(i))
			tower.UpsertPassword(key, password, PasswordAlgorithmPBKDF2, DefaultPasswordSaltLength)
		}
	})

	b.Run("UpsertPassword-Argon2id", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "bench_argon2id_" + string(rune(i))
			tower.UpsertPassword(key, password, PasswordAlgorithmArgon2id, DefaultPasswordSaltLength)
		}
	})

	// Setup passwords for verification benchmarks
	setupKeys := []string{"verify_bcrypt", "verify_scrypt", "verify_pbkdf2", "verify_argon2id"}
	algorithms := []PasswordAlgorithm{
		PasswordAlgorithmBcrypt,
		PasswordAlgorithmScrypt,
		PasswordAlgorithmPBKDF2,
		PasswordAlgorithmArgon2id,
	}

	for i, key := range setupKeys {
		tower.UpsertPassword(key, password, algorithms[i], DefaultPasswordSaltLength)
	}

	b.Run("VerifyPassword-Bcrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tower.VerifyPassword("verify_bcrypt", password)
		}
	})

	b.Run("VerifyPassword-Scrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tower.VerifyPassword("verify_scrypt", password)
		}
	})

	b.Run("VerifyPassword-PBKDF2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tower.VerifyPassword("verify_pbkdf2", password)
		}
	})

	b.Run("VerifyPassword-Argon2id", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tower.VerifyPassword("verify_argon2id", password)
		}
	})
}
