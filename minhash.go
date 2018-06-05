package reconcile

// MinHash is a structure used for a technique to quickly estimate how similar
// two sets are. The scheme was invented by Andrei Broder in 1997.
//
// Each party generates a signature of their set, and since the signature is
// significantly smaller, it can be exchanged and compared locally.
//
// A. Z. Broder, M. Charikar, A. M. Frieze, and M. Mitzenmacher.
// Min-wise independent permutations.
// J. Comput. Syst. Sci., 60:630–659, 2000.
type MinHash struct {
	keysize   int
	signature [][]uint64
	hashcount int
	keycount  int
}

// NewMinHash creates a new MinHash structure and initializes the signature
// required for the specified hashcount and keysize.
func NewMinHash(hashcount int, keysize int) *MinHash {
	// Initialise signature
	signature := make([][]uint64, hashcount)
	for row := range signature {
		signature[row] = make([]uint64, keysize*8)
	}

	return &MinHash{keysize, signature, hashcount, 0}
}

// Add updates the signature to include the desired key
func (mh *MinHash) Add(key []byte) {
	for hashseed := 0; hashseed < mh.hashcount; hashseed++ {
		sum := Sum128x32(key, uint32(hashseed))
		hash := uint64(sum[0]) % uint64(mh.hashcount)

		// For each byte of the key
		for byteindex, keybyte := range key {
			pattern := uint8(1)

			// For each bit in the key
			for bitindex := 0; bitindex < 8; bitindex++ {
				if keybyte&pattern != 0 {
					// If hash is greater than signature, then update
					index := (byteindex * 8) + bitindex
					if hash > mh.signature[hashseed][index] {
						mh.signature[hashseed][index] = hash
					}
				}

				pattern <<= 1
			}
		}
	}

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

			if mh.signature[row][col] == 0 || remote.signature[row][col] == 0 {
				continue
			}

			matches++
		}
	}

	width := bitwidth * int(mh.hashcount)
	keycountSum := mh.keycount + remote.keycount

	return keycountSum * (width - matches) / (width + matches)
}
