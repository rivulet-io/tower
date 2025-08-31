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

	db.CreateList("list")
	db.PushLeft("list", tower.PrimitiveBool(true))
	db.PushLeft("list", tower.PrimitiveInt(42))
	db.PushLeft("list", tower.PrimitiveString("hello"))
	db.PushLeft("list", tower.PrimitiveFloat(3.14))

	for i := 0; i < 4; i++ {
		val, _ := db.PopRight("list")
		switch val.Type() {
		case tower.TypeBool:
			b, _ := val.Bool()
			println("Bool:", b)
		case tower.TypeInt:
			n, _ := val.Int()
			println("Int:", n)
		case tower.TypeString:
			s, _ := val.String()
			println("String:", s)
		case tower.TypeFloat:
			f, _ := val.Float()
			println("Float:", f)
		}
	}
}
