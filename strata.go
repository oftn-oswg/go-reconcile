package reconcile

import (
	"encoding/json"
)

//TODO
//Error handling
//More efficient to ignore smallest levels when sets differ in size?

// Strata estimates the difference between two sets
type Strata struct {
	Cellsize int //IBF size for each strata
	Keysize  int
	Depth    int
	IBFset   []*IBF
}

//This is used for the JSON data transfer of the difference estimators
//For type Strata
type DifferenceSerialization []IBFSerialization

func NewStrata(cellsize, keysize, depth int) *Strata {
	IBFset := make([]*IBF, depth)
	return &Strata{cellsize, keysize, depth, IBFset}
}

//Populate an estimator in one
func (s *Strata) Populate(keys [][]byte) {
	//Create strata ibfs
	for d := 0; d < s.Depth; d++ {
		s.IBFset[d] = NewIBF(s.Cellsize, s.Keysize)
	}

	//assign elements by trailing zeroes
	for _, key := range keys {
		s.IBFset[TrailingZeroes(key[:3], uint(s.Depth-1))].Add(key)
	}
}

//Unmarshal JSON into DifferenceSerialization struct
func (s *Strata) UnmarshalStrataJSON(data []byte) error {
	serialization := make([]IBFSerialization, s.Depth)
	if err := json.Unmarshal(data, &serialization); err != nil {
		return err
	}

	//Process all JSON from remote strata estimator
	for level, _ := range serialization {
		s.IBFset[level].SetIBF(serialization[level])
	}

	return nil
}

func (s *Strata) MarshalStrataJSON() ([]byte, error) {
	signature := make([]IBFSerialization, s.Depth)

	//Process all JSON from remote strata estimator
	for level, _ := range signature {
		signature[level] = s.IBFset[level].GetIBF()

	}
	return json.Marshal(&signature)
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
	}
	return 0
}

//count trailing zeroes per bit up to limit
func TrailingZeroes(key []byte, limit uint) uint {
	var count uint = 0
	var pattern uint8 = 1

	for count < limit {
		pattern = 1 << (count % 8)
		if key[count/8]&pattern == 0 {
			count++
		} else {
			return count
		}
	}
	return count
}
