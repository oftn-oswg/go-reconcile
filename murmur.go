package reconcile

import (
	"unsafe"
)

func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// Sum128x32 computes the Murmur32 128-bit hash for the 32-bit platform.
// It assumes a little-endian architecture.
func Sum128x32(key []byte) [4]uint32 {
	const c1 = 0x239b961b
	const c2 = 0xab0e9789
	const c3 = 0x38b34ae5
	const c4 = 0xa1e38b93

	var h1, h2, h3, h4 uint32

	size := len(key)
	blocks := size / 16

	head := *(*[]uint32)(unsafe.Pointer(&key))
	tail := key[blocks*16:]

	// Head
	for i := 0; i < blocks; i++ {
		k1 := head[i*4+0]
		k2 := head[i*4+1]
		k3 := head[i*4+2]
		k4 := head[i*4+3]

		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2
		h1 ^= k1

		h1 = (h1 << 19) | (h1 >> 13) // rotl32(h1, 19)
		h1 += h2
		h1 = h1*5 + 0x561ccd1b

		k2 *= c2
		k2 = (k2 << 16) | (k2 >> 16) // rotl32(k2, 16)
		k2 *= c3
		h2 ^= k2

		h2 = (h2 << 17) | (h2 >> 15) // rotl32(h2, 17)
		h2 += h3
		h2 = h2*5 + 0x0bcaa747

		k3 *= c3
		k3 = (k3 << 17) | (k3 >> 15) // rotl32(k3, 17)
		k3 *= c4
		h3 ^= k3

		h3 = (h3 << 15) | (h3 >> 17) // rotl32(h3, 15)
		h3 += h4
		h3 = h3*5 + 0x96cd1c35

		k4 *= c4
		k4 = (k4 << 18) | (k4 >> 14) // rotl32(k4, 18)
		k4 *= c1
		h4 ^= k4

		h4 = (h4 << 13) | (h4 >> 19) // rotl32(h4, 13)
		h4 += h1
		h4 = h4*5 + 0x32ac3b17
	}

	var k1, k2, k3, k4 uint32

	// Tail
	switch size & 0xf {
	case 0xf:
		k4 ^= uint32(tail[14]) << 16
		fallthrough
	case 0xe:
		k4 ^= uint32(tail[13]) << 8
		fallthrough
	case 0xd:
		k4 ^= uint32(tail[12])
		k4 *= c4
		k4 = (k4 << 18) | (k4 >> 14) // rotl32(k4, 18)
		k4 *= c1
		h4 ^= k4
		fallthrough

	case 0xc:
		k3 ^= uint32(tail[11]) << 24
		fallthrough
	case 0xb:
		k3 ^= uint32(tail[10]) << 16
		fallthrough
	case 0xa:
		k3 ^= uint32(tail[9]) << 8
		fallthrough
	case 0x9:
		k3 ^= uint32(tail[8])
		k3 *= c3
		k3 = (k3 << 17) | (k3 >> 15) // rotl32(k3, 17)
		k3 *= c4
		h3 ^= k3
		fallthrough

	case 0x8:
		k2 ^= uint32(tail[7]) << 24
		fallthrough
	case 0x7:
		k2 ^= uint32(tail[6]) << 16
		fallthrough
	case 0x6:
		k2 ^= uint32(tail[5]) << 8
		fallthrough
	case 0x5:
		k2 ^= uint32(tail[4]) << 0
		k2 *= c2
		k2 = (k2 << 16) | (k2 >> 16) // rotl32(k2, 16)
		k2 *= c3
		h2 ^= k2
		fallthrough

	case 0x4:
		k1 ^= uint32(tail[3]) << 24
		fallthrough
	case 0x3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 0x2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 0x1:
		k1 ^= uint32(tail[0]) << 0
		k1 *= c1
		k1 = (k1 << 15) | (k1 >> 17) // rotl32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(size)
	h2 ^= uint32(size)
	h3 ^= uint32(size)
	h4 ^= uint32(size)

	h1 = h1 + h2
	h1 = h1 + h3
	h1 = h1 + h4
	h2 = h2 + h1
	h3 = h3 + h1
	h4 = h4 + h1

	h1 = fmix32(h1)
	h2 = fmix32(h2)
	h3 = fmix32(h3)
	h4 = fmix32(h4)

	h1 = h1 + h2
	h1 = h1 + h3
	h1 = h1 + h4
	h2 = h2 + h1
	h3 = h3 + h1
	h4 = h4 + h1

	return [4]uint32{h1, h2, h3, h4}
}
