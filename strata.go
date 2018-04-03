package reconcile

import (
	"fmt"
)

//TODO
//Error handling

// Strata estimates the difference between two sets
type Strata struct {
	Cellsize int //IBF size for each strata
	Keysize  int
	Depth    int
	IBFset   []*IBF
}

func NewStrata(cellsize, keysize, depth int) *Strata {
	IBFset := make([]*IBF, depth) //!
	return &Strata{cellsize, keysize, depth, IBFset}
}

func (s *Strata) Populate(keys [][]byte) {
	//Split up the keys into strata
	//Create strata ibfs
	for d := 0; d < s.Depth; d++ {
		s.IBFset[d] = NewIBF(s.Cellsize, s.Keysize)
	}

	//assign elements by trailing zeroes
	for _, key := range keys {
		s.IBFset[TrailingZeroes(key[:3], s.Depth-1)].Add(key)
	}
}

func (s *Strata) Estimate(remote *Strata) int {
	count := 0
	for level := len(s.IBFset) - 1; level >= -1; level-- {
		if level < 0 {
			return count
		}

		ibf := s.IBFset[level]
		remotelevel := remote.IBFset[level]
		ibf.Subtract(remotelevel)
		a, b, ok := ibf.Decode()

		if !ok {
			return (2 << uint(level)) * count
		}

		count += len(b) + len(a)
		fmt.Printf("Level %v (a,b,ok): (%v, %v, %v)\n", level+1, len(a), len(b), ok)
	}
	return 0
}

//count trailing zeroes per bit up to limit
func TrailingZeroes(key []byte, limit int) int {
	var count int = 0
	var pattern uint8 = 1
	index := 0
	for index < len(key) {
		if key[index]&pattern == 0 && count < limit {
			count++
			//if = 128 then goto next byte
			if pattern == 128 {
				pattern = 1
				index++
			} else {
				pattern <<= 1
			}
		} else {
			return count
		}
	}
	return count
}
