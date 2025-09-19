package main

import (
	"github.com/rivulet-io/tower/op"
	"github.com/rivulet-io/tower/util/size"
)

func main() {
	db, err := op.NewOperator(&op.Options{
		Path:         "data",
		FS:           op.InMemory(),
		CacheSize:    size.NewSizeFromMegabytes(64),
		MemTableSize: size.NewSizeFromMegabytes(16),
		BytesPerSync: size.NewSizeFromKilobytes(512),
	})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db.CreateList("my_list")
	db.PushLeft("my_list", op.PrimitiveString("hello1"))
	db.PushLeft("my_list", op.PrimitiveString("hello2"))
	db.PushLeft("my_list", op.PrimitiveString("hello3"))
	db.PushLeft("my_list", op.PrimitiveString("hello4"))
	db.PushLeft("my_list", op.PrimitiveString("hello5"))
	db.PushLeft("my_list", op.PrimitiveString("hello6"))
	db.PushRight("my_list", op.PrimitiveString("world1"))
	db.PushRight("my_list", op.PrimitiveString("world2"))
	db.PushRight("my_list", op.PrimitiveString("world3"))
	db.PushRight("my_list", op.PrimitiveString("world4"))
	db.PushRight("my_list", op.PrimitiveString("world5"))
	db.PushRight("my_list", op.PrimitiveString("world6"))

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
