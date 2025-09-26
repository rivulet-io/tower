package op

import (
	"testing"

	"github.com/rivulet-io/tower/util/size"
)

func TestNewTower(t *testing.T) {
	tests := []struct {
		name    string
		options *Options
		wantErr bool
	}{
		{
			name: "valid in-memory tower",
			options: &Options{
				Path:         "data",
				FS:           InMemory(),
				CacheSize:    size.NewSizeFromMegabytes(64),
				MemTableSize: size.NewSizeFromMegabytes(16),
				BytesPerSync: size.NewSizeFromKilobytes(512),
			},
			wantErr: false,
		},
		{
			name: "valid on-disk tower",
			options: &Options{
				Path:         "test_data",
				FS:           OnDisk(),
				CacheSize:    size.NewSizeFromMegabytes(32),
				MemTableSize: size.NewSizeFromMegabytes(8),
				BytesPerSync: size.NewSizeFromKilobytes(256),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tower, err := NewOperator(tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOperator() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tower != nil {
				defer tower.Close()
			}
		})
	}
}

func TestTowerBasicOperations(t *testing.T) {
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

	// Test set and get
	t.Run("set and get", func(t *testing.T) {
		key := "test_key"
		value := &DataFrame{}
		value.SetString("test_value")

		err := tower.set(key, value)
		if err != nil {
			t.Errorf("Failed to set value: %v", err)
		}

		retrieved, err := tower.get(key)
		if err != nil {
			t.Errorf("Failed to get value: %v", err)
		}

		retrievedStr, err := retrieved.String()
		if err != nil {
			t.Errorf("Failed to convert to string: %v", err)
		}

		originalStr, err := value.String()
		if err != nil {
			t.Errorf("Failed to convert original to string: %v", err)
		}

		if retrievedStr != originalStr {
			t.Errorf("Retrieved value %s doesn't match original %s", retrievedStr, originalStr)
		}
	})

	// Test delete
	t.Run("delete", func(t *testing.T) {
		key := "delete_test_key"
		value := &DataFrame{}
		value.SetString("delete_test_value")

		// Set the value
		err := tower.set(key, value)
		if err != nil {
			t.Errorf("Failed to set value: %v", err)
		}

		// Delete the value
		err = tower.delete(key)
		if err != nil {
			t.Errorf("Failed to delete value: %v", err)
		}

		// Try to get the deleted value
		_, err = tower.get(key)
		if err == nil {
			t.Error("Expected error when getting deleted key, but got none")
		}
	})

	// Test set with nil value
	t.Run("set nil value", func(t *testing.T) {
		key := "nil_test_key"
		err := tower.set(key, nil)
		if err == nil {
			t.Error("Expected error when setting nil value, but got none")
		}
	})
}

func TestTowerRangePrefix(t *testing.T) {
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

	// Set up test data
	prefix := "test_prefix:"
	testData := map[string]string{
		prefix + "key1": "value1",
		prefix + "key2": "value2",
		prefix + "key3": "value3",
		"other_key":     "other_value",
	}

	for key, value := range testData {
		df := &DataFrame{}
		df.SetString(value)
		err := tower.set(key, df)
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Test range with prefix
	var foundKeys []string
	var foundValues []string

	err = tower.rangePrefix(prefix, func(key string, df *DataFrame) error {
		foundKeys = append(foundKeys, key)
		value, err := df.String()
		if err != nil {
			return err
		}
		foundValues = append(foundValues, value)
		return nil
	})

	if err != nil {
		t.Errorf("rangePrefix failed: %v", err)
	}

	if len(foundKeys) != 3 {
		t.Errorf("Expected 3 keys with prefix, found %d", len(foundKeys))
	}

	// Check that all found keys have the expected prefix
	for _, key := range foundKeys {
		if len(key) <= len(prefix) || key[:len(prefix)] != prefix {
			t.Errorf("Key %s doesn't have expected prefix %s", key, prefix)
		}
	}
}

func TestTowerConcurrency(t *testing.T) {
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

	// Test concurrent locks
	t.Run("concurrent locks", func(t *testing.T) {
		key := "lock_test_key"

		// Test exclusive lock
		unlock1 := tower.lock(key)
		unlock1()

		// Test read lock
		unlock2 := tower.lock(key)
		unlock2()
	})
}

