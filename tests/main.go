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

	db.CreateList("my_list")
	db.PushLeft("my_list", tower.PrimitiveString("hello1"))
	db.PushLeft("my_list", tower.PrimitiveString("hello2"))
	db.PushLeft("my_list", tower.PrimitiveString("hello3"))
	db.PushLeft("my_list", tower.PrimitiveString("hello4"))
	db.PushLeft("my_list", tower.PrimitiveString("hello5"))
	db.PushLeft("my_list", tower.PrimitiveString("hello6"))
	db.PushRight("my_list", tower.PrimitiveString("world1"))
	db.PushRight("my_list", tower.PrimitiveString("world2"))
	db.PushRight("my_list", tower.PrimitiveString("world3"))
	db.PushRight("my_list", tower.PrimitiveString("world4"))
	db.PushRight("my_list", tower.PrimitiveString("world5"))
	db.PushRight("my_list", tower.PrimitiveString("world6"))

	values, err := db.ListRange("my_list", 0, -1)
	if err != nil {
		panic(err)
	}

	for _, v := range values {
		str, err := v.String()
		if err != nil {
			panic(err)
		}
		println(str)
	}

}
