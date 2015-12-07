package murmur

import (
	"unsafe"
)

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= uint32(0x85ebca6b)
	h ^= h >> 13
	h *= uint32(0xc2b2ae35)
	h ^= h >> 16
	return h
}

func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= uint64(0xff51afd7ed558ccd)
	k ^= k >> 33
	k *= uint64(0xc4ceb9fe1a85ec53)
	k ^= k >> 33
	return k
}

func rotl32(x, r uint32) uint32 {
	return (x << r) | (x >> (32 - r))
}

func rotl64(x, r uint64) uint64 {
	return (x << r) | (x >> (64 - r))
}

func Murmur3Hash32(data []byte, seed uint32) uint32 {
	nblocks := len(data) / 4
	h1 := uint32(seed)

	c1 := uint32(0xcc9e2d51)
	c2 := uint32(0x1b873593)
	//----------
	// body
	for i := 0; i < nblocks; i++ {
		k1 := *(*uint32)(unsafe.Pointer(&data[i*4]))
		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2

		h1 ^= k1
		h1 = rotl32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}
	//----------
	// tail
	tail := data[nblocks*4:]
	k1 := uint32(0)
	switch len(data) & 3 {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}
	//----------
	// finalization

	h1 ^= uint32(len(data))

	h1 = fmix32(h1)

	return h1
}

func Murmur3Hash128(p []byte, seed uint32) (uint64, uint64) {
	nblocks := len(p) / 16
	//seed := uint64(0)
	h1 := uint64(seed)
	h2 := uint64(seed)
	c1 := uint64(0x87c37b91114253d5)
	c2 := uint64(0x4cf5ad432745937f)
	for i := 0; i < nblocks; i++ {
		t := (*[2]uint64)(unsafe.Pointer(&p[i*16]))
		k1, k2 := t[0], t[1]
		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = rotl64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729
		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = rotl64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	//----------
	// tail
	tail := p[nblocks*16:]

	k1 := uint64(0)
	k2 := uint64(0)

	switch len(p) & 15 {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8]) << 0
		k2 *= c2
		k2 = rotl64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0]) << 0
		k1 *= c1
		k1 = rotl64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	//----------
	// finalization

	h1 ^= uint64(len(p))
	h2 ^= uint64(len(p))

	h1 += h2
	h2 += h1

	h1 = fmix64(h1)
	h2 = fmix64(h2)

	h1 += h2
	h2 += h1
	return h1, h2
}
