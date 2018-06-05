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
	hashcount int
	keycount  int
}

const signatureInit = math.MaxUint64

func NewMinHash(hashcount int, keysize int) *MinHash {
	//Initialise signature with maximum values
	signature := make([][]uint64, hashcount)
	for row := range signature {
		signature[row] = make([]uint64, keysize*8)
	}

	for hashseed := 0; hashseed < hashcount; hashseed++ { //0 to hashcount
		for bitindex := 0; bitindex < (keysize * 8); bitindex++ { //for each bit in key
			signature[hashseed][bitindex] = signatureInit
		} //scan through bits
	} //for each hash
	return &MinHash{keysize, signature, hashcount, 0}
}

func (mh *MinHash) Add(key []byte) {
	for hashseed := 0; hashseed < mh.hashcount; hashseed++ { //0 to hashcount
		sum := Sum128x32(key, uint32(hashseed))
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

// Difference calculates the expected size of the difference
func (mh *MinHash) Difference(remote *MinHash) int {
	matches := 0
	bitwidth := mh.keysize * 8

	// Count signature matches
	// For each hash/row in the signature
	for row := range mh.signature {
		// For each bit
		for col := 0; col < bitwidth; col++ {
			if mh.signature[row][col] != remote.signature[row][col] {
				continue
			}

			if mh.signature[row][col] == signatureInit || remote.signature[row][col] == signatureInit {
				continue
			}

			matches++
		}
	}

	width := bitwidth * int(mh.hashcount)
	keycountSum := mh.keycount + remote.keycount

	return keycountSum * (width - matches) / (width + matches)
}
