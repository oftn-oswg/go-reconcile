package reconcile

import (
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
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

func MinHashDifference(sigA [][]uint64, sigB [][]uint64) int {
	if len(sigA) != len(sigB) {
		fmt.Println("Hash count doesn't match")
		return 0 //should throw error
	}
	if len(sigA[0]) != len(sigB[0]) {
		fmt.Println("Keysizes don't match")
		return 0 //should throw error
	}

	diff := 0
	bitwidth := len(sigA[0])
	for row := range sigA {
		for col := 0; col < bitwidth; col++ {
			if sigA[row][col] == sigB[row][col] {
				if sigA[row][col] != math.MaxUint64 && sigB[row][col] != math.MaxUint64 {
					diff++
				}
			}
		}
	}

	return diff
}

//Should probably return a custom struct
func GetMinHashSignature(keys [][]byte, hashcount uint32) [][]uint64 {
	//dont precompute permutation hashset
	//build signature table of hashcount rows
	//hashseed is signature row
	keysize := len(keys[0])

	mh := make([][]uint64, hashcount)
	for row := range mh {
		mh[row] = make([]uint64, keysize*8)
	}

	var hashseed uint32

	//init signature with values (maximum)
	for hashseed = 0; hashseed < hashcount; hashseed++ { //0 to hashcount
		for bitindex := 0; bitindex < (keysize * 8); bitindex++ { //for each bit in key
			mh[hashseed][bitindex] = math.MaxUint64
		} //scan through bits
	} //for each hash

	for _, key := range keys {
		for hashseed = 0; hashseed < hashcount; hashseed++ { //0 to hashcount
			hash := murmur3.Sum64WithSeed(key, hashseed) % uint64(len(keys))
			for byteindex, keybyte := range key { //for each byte of key
				var pattern uint8 = 1
				for bitindex := 0; bitindex < 8; bitindex++ { //for each bit in key
					if keybyte&pattern != 0 { //do if found bit & not already found
						if hash < mh[hashseed][(byteindex*8)+bitindex] {
							mh[hashseed][(byteindex*8)+bitindex] = hash
						} //if hash is less than then update
					} //if bit is 1
					pattern <<= 1
				} //scan through bits
			} //scan through bytes of key
		} //for each hash
	} //for each key

	return mh
} //GetSignature
