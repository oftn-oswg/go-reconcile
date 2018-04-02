package reconcile

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spaolacci/murmur3"
)

type IBF struct {
	Size     int
	Keysize  int
	Hashset  []uint32
	Countset []int
	Bitset   []byte
}

type IBFSerialization struct {
	Size     int      `json:"size"`
	Keysize  int      `json:"keysize"`
	Hashset  []uint32 `json:"hashes"`
	Countset []int    `json:"counts"`
	Data     string   `json:"data"`
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
	hashset := make([]uint32, size)
	countset := make([]int, size)
	bitset := make([]byte, keysize*size)
	return &IBF{size, keysize, hashset, countset, bitset}
}

func (f *IBF) MarshalJSON() ([]byte, error) {
	return json.Marshal(&IBFSerialization{
		f.Size,
		f.Keysize,
		f.Hashset,
		f.Countset,
		hex.EncodeToString(f.Bitset),
	})
}

func (f *IBF) UnmarshalJSON(data []byte) error {
	serialization := &IBFSerialization{}
	if err := json.Unmarshal(data, serialization); err != nil {
		return err
	}

	bitset, err := hex.DecodeString(serialization.Data)
	if err != nil {
		return err
	}

	f.Size = serialization.Size
	f.Keysize = serialization.Keysize
	f.Hashset = serialization.Hashset
	f.Countset = serialization.Countset
	f.Bitset = bitset

	return nil
}

func (f *IBF) Hashes(key []byte) []uint32 {
	// Hash the key to get array indices
	h1, h2 := murmur3.Sum128(key)

	a := uint32(h1)       // low 32 bits of h1
	b := uint32(h1 >> 32) // high 32 bits of h1
	c := uint32(h2)       // low 32 bits of h2
	d := uint32(h2 >> 32) // high 32 bits of h2

	hashes := []uint32{a, b, c, d}

	return hashes
}

func (f *IBF) Indices(hashes []uint32) []int {
	indices := make([]int, len(hashes))
	for index, hash := range hashes {
		indices[index] = int(uint(hash) % uint(f.Size))
	}
	return indices
}

func (f *IBF) Update(key []byte, hash uint32, indices []int, incCount int) error {
	keysize := len(key)
	if keysize != f.Keysize {
		return fmt.Errorf("Update key '%s' of size %d to filter with key size of %d",
			key, keysize, f.Keysize)
	}

	for _, index := range indices {
		bitsetStart := index * f.Keysize
		for i := 0; i < keysize; i++ {
			f.Bitset[bitsetStart+i] ^= key[i]
		}
		f.Hashset[index] ^= hash
		f.Countset[index] += incCount
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
	if f.Size != subtrahend.Size {
		return errors.New("Subtracting two filters of differing size")
	}
	if f.Keysize != subtrahend.Keysize {
		return errors.New("Subtracting two filters with differing max key size")
	}

	// Subtract keyset
	keysetsize := len(f.Bitset)
	for i := 0; i < keysetsize; i++ {
		f.Bitset[i] ^= subtrahend.Bitset[i]
	}

	for i := 0; i < f.Size; i++ {
		// Subtract hashset
		f.Hashset[i] ^= subtrahend.Hashset[i]
		f.Countset[i] -= subtrahend.Countset[i]
	}

	return nil
}

func (f *IBF) Count(index int) int {
	return f.Countset[index]
}

func (f *IBF) KeySum(index int) []byte {
	keyindex := index * f.Keysize
	return f.Bitset[keyindex : keyindex+f.Keysize]
}

func (f *IBF) HashSum(index int) uint32 {
	return f.Hashset[index]
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
	for i := 0; i < f.Size; i++ {
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

		key := make([]byte, f.Keysize)
		copy(key, f.KeySum(index))

		count := f.Count(index)
		hashes := f.Hashes(key)
		indices := f.Indices(hashes[1:])

		// Use the value of count to determine which difference we are part of
		if count > 0 {
			diffA = append(diffA, key)
		} else {
			diffB = append(diffB, key)
		}

		// Remove this cell to uncover new pure cells
		f.Update(key, hashes[0], indices, -count)
		for _, i := range indices {
			if f.IsPure(i) {
				pureIndices = append(pureIndices, i)
			}

		}
	}

	// Check for failure; we need an empty filter after decoding
	for i := 0; i < f.Size; i++ {
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
