package reconcile

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spaolacci/murmur3"
)

// IBF is the stucture for the invertible bloom filter.
type IBF struct {
	Size     int
	Keysize  int
	Hashset  []uint32
	Countset []int
	Bitset   []byte
}

// IBFSerialization is used to transfer the IBF along the wire suitable for use
// in a JavaScript implementation.
type IBFSerialization struct {
	Size     int      `json:"size"`
	Keysize  int      `json:"keysize"`
	Hashset  []uint32 `json:"hashes"`
	Countset []int    `json:"counts"`
	Data     string   `json:"data"`
}

// NewIBF creates a new invertible bloom filter of the specified `size`, or the
// number of cells to create, and `keysize`, or the number of bytes used to
// store a key.
//
// For example, if you are using a cryptographic hash function like SHA256 to
// represent your keys, you will want to set `keysize` to 32 in order to store
// 256 bits.
//
// You might not want to call this function directly, as the best value of the
// `size` argument is roughly determined by the size of the set difference. This
// can be ascertained approximately with the stata estimator algorithm.
func NewIBF(size, keysize int) *IBF {
	if size < 1 {
		size = 1
	}
	if keysize < 1 {
		keysize = 1
	}

	hashset := make([]uint32, size)
	countset := make([]int, size)
	bitset := make([]byte, keysize*size)
	return &IBF{size, keysize, hashset, countset, bitset}
}

// MarshalJSON encodes the invertible bloom filter in a JSON byte format as
// documented by the IBFSerialization type.
func (f *IBF) MarshalJSON() ([]byte, error) {
	return json.Marshal(&IBFSerialization{
		f.Size,
		f.Keysize,
		f.Hashset,
		f.Countset,
		hex.EncodeToString(f.Bitset),
	})
}

// UnmarshalJSON decodes the invertible bloom filter from a JSON byte format as
// documented by the IBFSerialization type.
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

// Hashes returns an array of hash values resulting from the specified `key`.
// This implementation uses the 128-bit murmur3 hash and returns the following,
// in order:
//
// - the low 32 bits of the first part of the result
// - the high 32 bits of the first part of the result
// - the low 32 bits of the second part of the result
// - the high 32 bits of the second part of the result
//
// The first value is used only for the hash sum, while the following three
// values are used for indices.
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

// Indices converts an array of hash values into indices suitable for use in the
// filter. This implementation returns a new array where the elements are taken
// from the elements of the hash value array `mod` the size of the filter.
func (f *IBF) Indices(hashes []uint32) []int {
	indices := make([]int, len(hashes))
	for index, hash := range hashes {
		indices[index] = int(uint(hash) % uint(f.Size))
	}
	return indices
}

// Update changes the value of the filter at the indices specified from the
// `indices` argument. Each value at those indices hash its bitset XORed with
// the `key` argument, the count value is increased by `incCount`, and the
// hashset is XORed with the value of the `hash` argument.
//
// If the key is not of the proper length, this function returns an error.
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

// Add inserts the key into the filter. If the key is not of the proper length,
// this function returns an error.
func (f *IBF) Add(key []byte) error {
	hashes := f.Hashes(key)
	indices := f.Indices(hashes[1:])
	return f.Update(key, hashes[0], indices, 1)
}

// Remove removes the key from the filter. If the key is not of the proper
// length, this function returns an error.
func (f *IBF) Remove(key []byte) error {
	hashes := f.Hashes(key)
	indices := f.Indices(hashes[1:])
	return f.Update(key, hashes[0], indices, -1)
}

// Subtract performs the invertible bloom filter subtraction algorithm and
// stores the result into this filter. This function returns an error if the
// filters were initialized with a different size or keysize.
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

// Count returns the value of the count cell at the specified `index`.
func (f *IBF) Count(index int) int {
	return f.Countset[index]
}

// KeySum returns the value of the key sum cell at the specified `index`.
func (f *IBF) KeySum(index int) []byte {
	keyindex := index * f.Keysize
	keysum := make([]byte, f.Keysize)
	copy(keysum, f.Bitset[keyindex:])
	return keysum
}

// HashSum returns the value of the hash sum cell at the specified `index`.
func (f *IBF) HashSum(index int) uint32 {
	return f.Hashset[index]
}

// IsPure returns true if the cell has a count of 1 or -1, and that the hash sum
// value matches the hash of the cell's key sum.
//
// This indicates a good chance that only one element has been stored at the
// cell with this index, and that it may be uncovered.
func (f *IBF) IsPure(index int) bool {
	count := f.Count(index)
	if count != 1 && count != -1 {
		return false
	}

	keyindex := index * f.Keysize
	hashes := f.Hashes(f.Bitset[keyindex : keyindex+f.Keysize])
	return hashes[0] == f.HashSum(index)
}

// Decode performs the decoding operation for this invertible bloom filter.
// Suppose this filter is called `A`, and that we have called `A.Subtract(B)`.
// This function returns three values in order, where "∖" is the set difference:
// - Some subset of A ∖ B,
// - Some subset of B ∖ A,
// - An indication of whether all the elements have been properly decoded.
//
// The process of decoding changes the filter. The filter removes all keys that
// have been successfully decoded. So it will be empty if all elements were
// decoded.
func (f *IBF) Decode() (a [][]byte, b [][]byte, ok bool) {
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

		key := f.KeySum(index)
		count := f.Count(index)
		hashes := f.Hashes(key)
		indices := f.Indices(hashes[1:])

		// Use the value of count to determine which difference we are part of
		if count > 0 {
			a = append(a, key)
		} else {
			b = append(b, key)
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
			return
		}
	}
	for _, v := range f.Bitset {
		if v != 0 {
			return
		}
	}

	ok = true
	return
}
