package sparsehash

type HashKey interface {
	HashCode() uint64
	Equals(other interface{}) bool
}

type HashMapStore interface {
	KeyEqual(i int, key interface{}) bool
	Del(i int)
	ResetSize(size int)
}

type HashMap interface {
	Clear()
	Size() int
	Get(key interface{}) interface{}
	Del(key interface{}) (bool, interface{})
	Put(key interface{}, value interface{}) (bool, interface{})
}
