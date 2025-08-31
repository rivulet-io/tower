package tower

import (
	"sync"
	"testing"
)

func TestConcurrentMapBasicOperations(t *testing.T) {
	cm := NewConcurrentMap[string, int]()

	// Test Store and Load
	t.Run("store and load", func(t *testing.T) {
		key := "test_key"
		value := 42

		cm.Store(key, value)

		loaded, ok := cm.Load(key)
		if !ok {
			t.Error("Expected to load stored value")
		}
		if loaded != value {
			t.Errorf("Expected %d, got %d", value, loaded)
		}
	})

	// Test Load non-existent key
	t.Run("load non-existent key", func(t *testing.T) {
		_, ok := cm.Load("non_existent")
		if ok {
			t.Error("Expected false when loading non-existent key")
		}
	})

	// Test Delete
	t.Run("delete", func(t *testing.T) {
		key := "delete_key"
		value := 100

		cm.Store(key, value)
		cm.Delete(key)

		_, ok := cm.Load(key)
		if ok {
			t.Error("Expected false after deleting key")
		}
	})
}

func TestConcurrentMapLoadOrStore(t *testing.T) {
	cm := NewConcurrentMap[string, int]()

	// Test LoadOrStore with new key
	t.Run("load or store new key", func(t *testing.T) {
		key := "new_key"
		value := 123

		actual, loaded := cm.LoadOrStore(key, value)
		if loaded {
			t.Error("Expected loaded to be false for new key")
		}
		if actual != value {
			t.Errorf("Expected %d, got %d", value, actual)
		}
	})

	// Test LoadOrStore with existing key
	t.Run("load or store existing key", func(t *testing.T) {
		key := "existing_key"
		originalValue := 456
		newValue := 789

		cm.Store(key, originalValue)

		actual, loaded := cm.LoadOrStore(key, newValue)
		if !loaded {
			t.Error("Expected loaded to be true for existing key")
		}
		if actual != originalValue {
			t.Errorf("Expected %d (original), got %d", originalValue, actual)
		}
	})
}

func TestConcurrentMapRange(t *testing.T) {
	cm := NewConcurrentMap[string, int]()

	// Store test data
	testData := map[string]int{
		"key1": 1,
		"key2": 2,
		"key3": 3,
	}

	for k, v := range testData {
		cm.Store(k, v)
	}

	// Test Range
	t.Run("range over all items", func(t *testing.T) {
		found := make(map[string]int)

		cm.Range(func(key string, value int) bool {
			found[key] = value
			return true // continue iteration
		})

		if len(found) != len(testData) {
			t.Errorf("Expected %d items, found %d", len(testData), len(found))
		}

		for k, v := range testData {
			if found[k] != v {
				t.Errorf("Key %s: expected %d, got %d", k, v, found[k])
			}
		}
	})

	// Test Range with early termination
	t.Run("range with early termination", func(t *testing.T) {
		count := 0

		cm.Range(func(key string, value int) bool {
			count++
			return count < 2 // stop after 2 items
		})

		if count != 2 {
			t.Errorf("Expected to visit 2 items, visited %d", count)
		}
	})
}

func TestConcurrentMapConcurrency(t *testing.T) {
	cm := NewConcurrentMap[int, string]()
	numGoroutines := 100
	numOperations := 1000

	var wg sync.WaitGroup

	// Test concurrent writes
	t.Run("concurrent writes", func(t *testing.T) {
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := id*numOperations + j
					value := string(rune('A' + (key % 26)))
					cm.Store(key, value)
				}
			}(i)
		}

		wg.Wait()
	})

	// Test concurrent reads
	t.Run("concurrent reads", func(t *testing.T) {
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := id*numOperations + j
					cm.Load(key)
				}
			}(i)
		}

		wg.Wait()
	})

	// Test mixed operations
	t.Run("mixed operations", func(t *testing.T) {
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations/10; j++ {
					key := id*(numOperations/10) + j
					value := string(rune('A' + (key % 26)))

					// Store
					cm.Store(key, value)

					// Load
					cm.Load(key)

					// LoadOrStore
					cm.LoadOrStore(key, value+"_new")

					// Delete
					if j%2 == 0 {
						cm.Delete(key)
					}
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestConcurrentMapTypes(t *testing.T) {
	// Test with different types
	t.Run("string to string", func(t *testing.T) {
		cm := NewConcurrentMap[string, string]()
		cm.Store("hello", "world")

		value, ok := cm.Load("hello")
		if !ok || value != "world" {
			t.Errorf("Expected 'world', got %s", value)
		}
	})

	t.Run("int to struct", func(t *testing.T) {
		type TestStruct struct {
			Name string
			Age  int
		}

		cm := NewConcurrentMap[int, TestStruct]()
		testStruct := TestStruct{Name: "John", Age: 30}

		cm.Store(1, testStruct)

		value, ok := cm.Load(1)
		if !ok {
			t.Error("Expected to load struct")
		}
		if value.Name != "John" || value.Age != 30 {
			t.Errorf("Struct values don't match: got %+v", value)
		}
	})
}
