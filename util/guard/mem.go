package guard

import (
	"fmt"

	"github.com/awnumar/memguard"
	"github.com/rivulet-io/tower/util/synx"
)

type MemoryBuffer struct {
	m *synx.ConcurrentMap[string, *memguard.Enclave]
}

func NewMemoryBuffer() *MemoryBuffer {
	return &MemoryBuffer{
		m: synx.NewConcurrentMap[string, *memguard.Enclave](),
	}
}

func (mb *MemoryBuffer) Set(key string, data []byte) {
	lb := memguard.NewBufferFromBytes(data)
	ec := lb.Seal()
	mb.m.Store(key, ec)
}

func (mb *MemoryBuffer) Use(key string, fn func(data []byte) error) error {
	ec, ok := mb.m.Load(key)
	if !ok {
		return fmt.Errorf("key %s not found", key)
	}

	lb, err := ec.Open()
	if err != nil {
		return fmt.Errorf("failed to open enclave: %w", err)
	}

	defer lb.Destroy()
	return fn(lb.Bytes())
}

func (mb *MemoryBuffer) Delete(key string) {
	mb.m.Delete(key)
}
