package tower

import (
	"fmt"

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
	db *pebble.DB
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

	return &Tower{db: db}, nil
}

func (t *Tower) Close() error {
	return t.db.Close()
}
