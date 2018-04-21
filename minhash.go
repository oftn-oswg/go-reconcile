package reconcile

import (
	"math"
)

//Using Min Hash
//Each party generates a signature
//Exchange and compare locally

//For each key (row)
//For each hash compute h(row)
//then
//For each bit in row
//If bit==1
//for each hash function
//if h(row) < M(i, c)
//M(i,c) = h(row)

type MinHash struct {
	keysize   int
	signature [][]uint64
	hashcount uint32
	keycount  int
}

func NewMinHash(hashcount uint32, keysize int) *MinHash {
	var hashseed uint32

	//Initialise signature with maximum values
	signature := make([][]uint64, hashcount)
	for row := range signature {
		signature[row] = make([]uint64, keysize*8)
	}

	for hashseed = 0; hashseed < hashcount; hashseed++ { //0 to hashcount
		for bitindex := 0; bitindex < (keysize * 8); bitindex++ { //for each bit in key
			signature[hashseed][bitindex] = math.MaxUint64
		} //scan through bits
	} //for each hash
	return &MinHash{keysize, signature, hashcount, 0}
}

func (mh *MinHash) Add(key []byte) {
	var hashseed uint32

	for hashseed = 0; hashseed < mh.hashcount; hashseed++ { //0 to hashcount
		sum := Sum128x32(key, hashseed)
		hash := uint64(sum[0]) % uint64(mh.hashcount)
		for byteindex, keybyte := range key { //for each byte of key
			var pattern uint8 = 1
			for bitindex := 0; bitindex < 8; bitindex++ { //for each bit in key
				if keybyte&pattern != 0 { //do if found bit & not already found
					if hash < mh.signature[hashseed][(byteindex*8)+bitindex] {
						mh.signature[hashseed][(byteindex*8)+bitindex] = hash
					} //if hash is less than then update
				} //if bit is 1
				pattern <<= 1
			} //scan through bits
		} //scan through bytes of key
	} //for each hash
	mh.keycount++
}

//Returns Jacquard similiarity score
func (mh *MinHash) Difference(remote *MinHash) int {
	//Catch errors

	match := 0
	bitwidth := mh.keysize * 8
	for row := range mh.signature {
		for col := 0; col < bitwidth; col++ {
			if mh.signature[row][col] == remote.signature[row][col] {
				if mh.signature[row][col] != math.MaxUint64 && remote.signature[row][col] != math.MaxUint64 {
					match++
				}
			} //if they match
		} //for each bit
	} //for each hash/row in signature
	score := float64(match) / float64(bitwidth*int(mh.hashcount))
	return int(((1.0 - score) / (1.0 + score)) * float64(mh.keycount+remote.keycount))
}
