package op

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/rivulet-io/tower/util/size"
	"github.com/rivulet-io/tower/util/synx"
)

type Options struct {
	Path         string
	BytesPerSync size.Size
	CacheSize    size.Size
	MemTableSize size.Size
	FS           vfs.FS
}

func InMemory() vfs.FS {
	return vfs.NewMem()
}

func OnDisk() vfs.FS {
	return vfs.Default
}

type Operator struct {
	db      *pebble.DB
	lockers *synx.ConcurrentMap[string, *sync.RWMutex]
}

func NewOperator(opt *Options) (*Operator, error) {
	options := &pebble.Options{
		FS:           opt.FS,
		BytesPerSync: int(opt.BytesPerSync),
		Cache:        pebble.NewCache(opt.CacheSize.Bytes()),
		MemTableSize: uint64(opt.MemTableSize.Bytes()),
	}

	db, err := pebble.Open(opt.Path, options)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}

	return &Operator{
		db:      db,
		lockers: synx.NewConcurrentMap[string, *sync.RWMutex](),
	}, nil
}

func (op *Operator) Close() error {
	return op.db.Close()
}

func (op *Operator) lock(key string) (unlock func()) {
	locker, _ := op.lockers.LoadOrStore(key, &sync.RWMutex{})
	locker.Lock()
	return func() {
		locker.Unlock()
	}
}

func (op *Operator) set(key string, value *DataFrame) error {
	if value == nil {
		return fmt.Errorf("value cannot be nil")
	}

	data, err := value.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal dataframe: %w", err)
	}

	if err := op.db.Set([]byte(key), data, nil); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) get(key string) (*DataFrame, error) {
	data, closer, err := op.db.Get([]byte(key))
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}
	defer closer.Close()

	df, err := UnmarshalDataFrame(data)
	if err != nil {
		if isReal := IsDataframeExpiredError(err); isReal != nil {
			_ = op.smartDelete(key, df.typ) // Clean up expired data
		}

		return nil, fmt.Errorf("failed to unmarshal dataframe for key %s: %w", key, err)
	}

	return df, nil
}

func (op *Operator) delete(key string) error {
	if err := op.db.Delete([]byte(key), nil); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

func (op *Operator) Remove(key string) error {
	return op.delete(key)
}

func (op *Operator) smartDelete(key string, dataType DataType) error {
	switch dataType {
	case TypeList:
		return op.DeleteList(key)
	case TypeMap:
		return op.DeleteMap(key)
	case TypeSet:
		return op.DeleteSet(key)
	case TypeTimeseries:
		return op.DeleteTimeSeries(key)
	case TypeBloomFilter:
		return op.DeleteBloomFilter(key)
	}

	return op.delete(key)
}

func (op *Operator) rangePrefix(prefix string, fn func(key string, df *DataFrame) error) error {
	iter, err := op.db.NewIter(&pebble.IterOptions{
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
