package tower

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

type Options struct {
	Path         string
	BytesPerSync Size
	CacheSize    Size
	MemTableSize Size
	FS           vfs.FS
}

func InMemory() vfs.FS {
	return vfs.NewMem()
}

func OnDisk() vfs.FS {
	return vfs.Default
}

type Tower struct {
	db      *pebble.DB
	lockers *ConcurrentMap[string, *sync.RWMutex]
}

func NewTower(opt *Options) (*Tower, error) {
	options := &pebble.Options{
		FS:           opt.FS,
		BytesPerSync: int(opt.BytesPerSync),
		Cache:        pebble.NewCache(opt.CacheSize.Bytes()),
		MemTableSize: uint64(opt.MemTableSize.Bytes()),
	}

	db, err := pebble.Open("tower.db", options)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Tower{
		db:      db,
		lockers: NewConcurrentMap[string, *sync.RWMutex](),
	}, nil
}

func (t *Tower) Close() error {
	return t.db.Close()
}

func (t *Tower) lock(key string) (unlock func()) {
	locker, _ := t.lockers.LoadOrStore(key, &sync.RWMutex{})
	locker.Lock()
	return func() {
		locker.Unlock()
	}
}

func (t *Tower) rlock(key string) (unlock func()) {
	locker, _ := t.lockers.LoadOrStore(key, &sync.RWMutex{})
	locker.RLock()
	return func() {
		locker.RUnlock()
	}
}
