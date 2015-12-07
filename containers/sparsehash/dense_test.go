package sparsehash

import (
	"testing"
)

func TestMapGetPut(t *testing.T) {
	hmap := make(map[int]int)
	loop := 1000000
	for i := 0; i < loop; i++ {
		//hmap.Put(i, i*100)
		hmap[i] = i * 100
	}
	for i := 0; i < loop; i++ {
		v, ok := hmap[i]
		if !ok || v != i*100 {
			t.Fatalf("Invalid value %v, expected:%d", v, i*100)
		}
	}
}

func TestDenseGetPut(t *testing.T) {
	hmap := NewDenseHashmap(0.5, 0.1)
	loop := 1000000
	for i := 0; i < loop; i++ {
		hmap.Put(i, i*100)
	}
	for i := 0; i < loop; i++ {
		v := hmap.Get(i)
		if v != i*100 {
			t.Fatalf("Invalid value %v, expected:%d", v, i*100)
		}
	}
	// if hmap.Size() != loop {
	// 	t.Fatalf("Invalid size %d, expected:%d", hmap.Size(), loop)
	// }
}
