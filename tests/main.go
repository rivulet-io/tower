package main

import (
	"github.com/rivulet-io/tower"
)

func main() {
	db, err := tower.NewTower(&tower.Options{
		Path:         "data",
		FS:           tower.InMemory(),
		CacheSize:    tower.NewSizeFromMegabytes(64),
		MemTableSize: tower.NewSizeFromMegabytes(16),
		BytesPerSync: tower.NewSizeFromKilobytes(512),
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.CreateMap("example_map")
	db.MapSet("example_map", tower.PrimitiveString("key1"), tower.PrimitiveString("value1"))
	value, _ := db.MapGet("example_map", tower.PrimitiveString("key1"))
	strValue, _ := value.String()
	println("Retrieved value:", strValue)
}
