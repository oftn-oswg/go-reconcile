package differencer

import (
	"errors"
	"fmt"

	"github.com/spaolacci/murmur3"
)

type IBF struct {
	size    int
	keysize int
	counts  []int
	keyset  []byte
	hashset []uint32
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func New(size, keysize int) *IBF {
	size = max(1, size)
	keysize = max(1, keysize)
	counts := make([]int, size)
	keyset := make([]byte, keysize*size)
	hashset := make([]uint32, size)
	return &IBF{size, keysize, counts, keyset, hashset}
}

func (f *IBF) Hashes(key []byte) []uint32 {
	// Hash the key to get array indices
	h1, h2 := murmur3.Sum128(key)

	a := uint32(h1)       // low 32 bits of h1
	b := uint32(h1 >> 32) // high 32 bits of h1
	c := uint32(h2)       // low 32 bits of h2
	d := uint32(h2 >> 32) // high 32 bits of h2

	return []uint32{a, b, c, d}
}

func (f *IBF) Indices(hashes []uint32) []int {
	indices := make([]int, len(hashes))
	for index, hash := range hashes {
		indices[index] = int(uint(hash) % uint(f.size))
	}
	return indices
}

func (f *IBF) Update(key []byte, hash uint32, indices []int, incCount int) error {
	keysize := len(key)
	if keysize != f.keysize {
		return fmt.Errorf("Update key '%s' of size %d to filter with key size of %d",
			key, keysize, f.keysize)
	}

	for _, index := range indices {
		keysetstart := index + f.keysize - keysize
		for i := 0; i < keysize; i++ {
			f.keyset[keysetstart+i] ^= key[i]
		}
		f.hashset[index] ^= hash
		f.counts[index] += incCount
	}

	return nil
}

func (f *IBF) Add(key []byte) error {
	hashes := f.Hashes(key)
	indices := f.Indices(hashes[1:])
	return f.Update(key, hashes[0], indices, 1)
}

func (f *IBF) Remove(key []byte) error {
	hashes := f.Hashes(key)
	indices := f.Indices(hashes[1:])
	return f.Update(key, hashes[0], indices, -1)
}

func (f *IBF) Subtract(subtrahend *IBF) error {
	if f.size != subtrahend.size {
		return errors.New("Subtracting two filters of differing size")
	}
	if f.keysize != subtrahend.keysize {
		return errors.New("Subtracting two filters with differing max key size")
	}

	// Subtract keyset
	keysetsize := len(f.keyset)
	for i := 0; i < keysetsize; i++ {
		f.keyset[i] ^= subtrahend.keyset[i]
	}

	for i := 0; i < f.size; i++ {
		// Subtract hashset
		f.hashset[i] ^= subtrahend.hashset[i]
		f.counts[i] -= subtrahend.counts[i]
	}

	return nil
}

func (f *IBF) Count(index int) int {
	return f.counts[index]
}

func (f *IBF) KeySum(index int) []byte {
	keyindex := index * f.keysize
	return f.keyset[keyindex : keyindex+f.keysize]
}

func (f *IBF) HashSum(index int) uint32 {
	return f.hashset[index]
}

func (f *IBF) IsPure(index int) bool {
	count := f.Count(index)
	if count != 1 && count != -1 {
		return false
	}

	hashes := f.Hashes(f.KeySum(index))
	return hashes[0] == f.HashSum(index)
}

func (f *IBF) Decode() ([][]byte, [][]byte, error) {
	diffA := [][]byte{}
	diffB := [][]byte{}
	pureIndices := []int{}

	// Get the initial list of pure cells
	for i := 0; i < f.size; i++ {
		if f.IsPure(i) {
			pureIndices = append(pureIndices, i)
		}
	}

	// Main decoding loop
	// Run while we have pure cells we can use to decode
	for len(pureIndices) > 0 {
		// Get one of the pure cell indices and dequeue
		index := pureIndices[len(pureIndices)-1]
		pureIndices = pureIndices[:len(pureIndices)-1]

		if !f.IsPure(index) {
			continue
		}

		key := f.KeySum(index)
		count := f.Count(index)

		// Use the value of count to determine which difference we are part of
		if count > 0 {
			diffA = append(diffA, key)
		} else {
			diffB = append(diffB, key)
		}

		hashes := f.Hashes(key)
		indices := f.Indices(hashes[1:])

		// Remove this cell to uncover new pure cells
		f.Update(key, hashes[0], indices, -count)
		for i := range indices {
			if f.IsPure(i) {
				pureIndices = append(pureIndices, i)
			}

		}
	}

	// Check for failure; we need an empty filter after decoding
	for i := 0; i < f.size; i++ {
		if f.HashSum(i) != 0 || f.Count(i) != 0 {
			return diffA, diffB, errors.New("Could not decode all elements")
		}
		for _, v := range f.KeySum(i) {
			if v != 0 {
				return diffA, diffB, errors.New("Coult not decode all elements")
			}
		}
	}

	return diffA, diffB, nil
}
