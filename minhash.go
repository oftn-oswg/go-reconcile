package reconcile

import "errors"

// ErrMinHashSize occurs when the signature lengths are different,
// and so they cannot be compared.
var ErrMinHashSize = errors.New("Mismatched signature sizes")

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
	signature []uint32
	keycount  int
}

// NewMinHash creates a new MinHash structure and initializes the signature
// required for the specified hashcount and keysize.
//
// TODO: Consider b-bit Minwise Hashing https://arxiv.org/pdf/0910.3349.pdf
func NewMinHash(hashcount int) *MinHash {
	// Initialise signature
	signature := make([]uint32, hashcount)
	return &MinHash{signature, 0}
}

// Add updates the signature to include the desired key
func (mh *MinHash) Add(key []byte) {
	hashcount := len(mh.signature)

	for seed := 0; seed < hashcount; seed++ {
		// TODO: XOR with pregenerated data instead of calling this hash
		// function `hashcount` times.
		sum := Sum128x32(key, uint32(seed))
		hash := sum[0]

		// Take the maximum so we can rely on zero-initialization.
		// In reality, this is a MaxHash.
		if hash > mh.signature[seed] {
			mh.signature[seed] = hash
		}
	}

	mh.keycount++
}

// Similarity computes the expected Jaccard similarity coefficient.
func (mh *MinHash) Similarity(remote *MinHash) (float64, error) {
	matches := 0
	total := len(mh.signature)

	// Remote MinHash must have same signature size.
	if len(remote.signature) != total {
		return 0, ErrMinHashSize
	}

	// Count signature matches
	for i := 0; i < total; i++ {
		l, r := mh.signature[i], remote.signature[i]
		if l == r {
			matches++
		}
	}

	return float64(matches) / float64(total), nil
}

// Estimate calculates the expected size of the set intersection.
func (mh *MinHash) Estimate(remote *MinHash) (int, error) {
	matches := 0
	total := len(mh.signature)

	// Remote MinHash must have same signature size.
	if len(remote.signature) != total {
		return 0, ErrMinHashSize
	}

	// Count signature matches
	for i := 0; i < total; i++ {
		l, r := mh.signature[i], remote.signature[i]
		if l == r {
			matches++
		}
	}

	// Our Jaccard similarity coefficient is equal to
	// |A ∩ B| over |A| + |B| - |A ∩ B|
	// which is estimated by matches / total.
	//
	// Solving for |A ∩ B| yields
	// (|A| + |B|) * (total - matches) / (total + matches)

	keycountSum := mh.keycount + remote.keycount
	return keycountSum * (total - matches) / (total + matches), nil
}
