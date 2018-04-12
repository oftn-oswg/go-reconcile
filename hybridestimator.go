package reconcile

import (
	"fmt"
	"math"
)

type HybridEstimator struct {
	Depth      int
	Keysize    int
	IBFset     []*IBF
	MinHashset []*MinHash
}

//Combines the Strata and MinHash Estimators for size of difference

func NewHybridEstimator(keys [][]byte) *HybridEstimator {
	depth := int(math.Ceil(math.Log2(float64(len(keys)))))
	IBFset := make([]*IBF, depth-2)
	MinHashPair := make([]*MinHash, 2)
	return &HybridEstimator{depth, len(keys[0]), IBFset, MinHashPair}
}

func (h *HybridEstimator) BuildSignature(keys [][]byte) {
	//Split up the keys by trailing zeroes
	//If 0 or 1 then use min hash
	for d := 0; d < h.Depth; d++ {
		if d > 1 {
			h.IBFset[d-2] = NewIBF(80, h.Keysize)
		}
	}

	h.MinHashset[0] = NewMinHash(100, len(keys[0]))
	h.MinHashset[1] = NewMinHash(100, len(keys[0]))

	//assign elements by trailing zeroes
	for _, key := range keys {
		tz := TrailingZeroes(key[:3], uint(h.Depth-1))
		if tz > 1 {
			h.IBFset[tz-2].Add(key)
		} else {
			h.MinHashset[tz].Add(key)
		}
	}

}

func (h *HybridEstimator) EstimateSizeDifference(remote *HybridEstimator) int {

	count := 0

	for level := h.Depth - 1; level >= -1; level-- {
		if level < 0 {
			return count
		} else if level < 2 { //MinHash
			mh := h.MinHashset[level]
			fmt.Println(mh.Difference(remote.MinHashset[level]))

		} else { //IBF Strata

			ibf := h.IBFset[level]
			remotelevel := remote.IBFset[level]
			ibf.Subtract(remotelevel)
			a, b, ok := ibf.Decode()
			if !ok {
				return (2 << uint(level)) * count
			}
			count += len(b) + len(a)
		}
	}
	return 0
}
