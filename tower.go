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

func (t *Tower) set(key string, value *DataFrame) error {
	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	data, err := value.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal dataframe: %w", err)
	}

	if err := t.db.Set([]byte(key), data, nil); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) get(key string) (*DataFrame, error) {
	data, closer, err := t.db.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}
	defer closer.Close()

	df, err := UnmarshalDataFrame(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal dataframe for key %s: %w", key, err)
	}

	return df, nil
}

func (t *Tower) delete(key string) error {
	if err := t.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

func (t *Tower) rangePrefix(prefix string, fn func(key string, df *DataFrame) error) error {
	iter, err := t.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		df, err := UnmarshalDataFrame(iter.Value())
		if err != nil {
			return fmt.Errorf("failed to unmarshal dataframe for key %s: %w", key, err)
		}
		if err := fn(key, df); err != nil {
			return fmt.Errorf("callback error for key %s: %w", key, err)
		}
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterator error: %w", err)
	}

	return nil
}
