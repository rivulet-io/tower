package tower

import "sync"

type ConcurrentMap[K comparable, V any] struct {
	data sync.Map
}

func NewConcurrentMap[K comparable, V any]() *ConcurrentMap[K, V] {
	return &ConcurrentMap[K, V]{}
}

func (cm *ConcurrentMap[K, V]) Load(key K) (V, bool) {
	value, ok := cm.data.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	asserted, ok := value.(V)
	return asserted, ok
}

func (cm *ConcurrentMap[K, V]) Store(key K, value V) {
	cm.data.Store(key, value)
}

func (cm *ConcurrentMap[K, V]) Delete(key K) {
	cm.data.Delete(key)
}

func (cm *ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	cm.data.Range(func(k, v any) bool {
		key, ok1 := k.(K)
		value, ok2 := v.(V)
		if !ok1 || !ok2 {
			return true // skip if type assertion fails
		}
		return f(key, value)
	})
}

func (cm *ConcurrentMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	actual, loaded := cm.data.LoadOrStore(key, value)
	if loaded {
		asserted, ok := actual.(V)
		if !ok {
			var zero V
			return zero, true
		}
		return asserted, true
	}
	return value, false
}

func (cm *ConcurrentMap[K, V]) LoadAndDelete(key K) (V, bool) {
	value, loaded := cm.data.LoadAndDelete(key)
	if !loaded {
		var zero V
		return zero, false
	}
	asserted, ok := value.(V)
	if !ok {
		var zero V
		return zero, false
	}
	return asserted, true
}

func (cm *ConcurrentMap[K, V]) Clear() {
	cm.data = sync.Map{}
}
