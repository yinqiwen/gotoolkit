package murmur

import (
	//"fmt"
	"testing"
)

func TestMurmur3Hash128(t *testing.T) {
	seed := uint32(789235)
	v := []byte("hello,world")
	t.Logf("%d", Murmur3Hash32(v, seed))
	h1, h2 := Murmur3Hash128(v, seed)
	t.Logf("%d %d", h1, h2)
	h1, h2 = Murmur3Hash128(v, seed)
	t.Logf("%d %d", h1, h2)
	v = []byte("hello,world,sdasdasdasdasdasdasdasdfwqeewqewqrweqreqwrtewtrewtwert")
	t.Logf("%d", Murmur3Hash32(v, seed))
	h1, h2 = Murmur3Hash128(v, seed)
	t.Logf("%d %d", h1, h2)

}
