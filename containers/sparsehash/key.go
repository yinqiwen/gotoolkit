package sparsehash

import (
	"math/rand"
	"unsafe"

	"github.com/yinqiwen/gotoolkit/algorithm/murmur"
)

const (
	MURMUR3_SEED = uint32(786523)
)

func hash(v interface{}) uint64 {
	switch v.(type) {
	case string:
		return uint64(murmur.Murmur3Hash32([]byte(v.(string)), MURMUR3_SEED))
	case []byte:
		return uint64(murmur.Murmur3Hash32(v.([]byte), MURMUR3_SEED))
	case bool:
		if v.(bool) {
			return 1
		} else {
			return 0
		}
	case int:
		return uint64(v.(int))
	case int8:
		return uint64(v.(int8))
	case int16:
		return uint64(v.(int16))
	case int32:
		return uint64(v.(int32))
	case int64:
		return uint64(v.(int64))
	case uint:
		return uint64(v.(uint))
	case uint8:
		return uint64(v.(uint8))
	case uint16:
		return uint64(v.(uint16))
	case uint32:
		return uint64(v.(uint32))
	case uint64:
		return v.(uint64)
	case float32:
		//Nan != Nan, so use a rand number to generate hash code
		if v != v {
			v = rand.Float32()
		}
		return uint64(murmur.Murmur3Hash32((*((*[4]byte)(unsafe.Pointer(&v))))[:], MURMUR3_SEED))
	case float64:
		//Nan != Nan, so use a rand number to generate hash code
		if v != v {
			v = rand.Float64()
		}
		return uint64(murmur.Murmur3Hash32((*((*[8]byte)(unsafe.Pointer(&v))))[:], MURMUR3_SEED))
	case complex64:
		return uint64(murmur.Murmur3Hash32((*((*[8]byte)(unsafe.Pointer(&v))))[:], MURMUR3_SEED))
	case complex128:
		return uint64(murmur.Murmur3Hash32((*((*[16]byte)(unsafe.Pointer(&v))))[:], MURMUR3_SEED))
	default:
		if hashable, ok := v.(HashKey); ok {
			return hashable.HashCode()
		} else {
			panic("Not support type for hashcode")
		}
	}
}

func equals(a interface{}, b interface{}) bool {
	return a == b
}
