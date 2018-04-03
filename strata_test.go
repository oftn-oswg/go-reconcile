package reconcile

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestStrata(t *testing.T) {

	numDifferences := 4
	numBaseElements := 50000
	keysize := 32
	cellSize := 80

	localkeys := [][]byte{}
	remotekeys := [][]byte{}

	for i := 0; i < numBaseElements; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			t.Error("Could not get random bytes for set element")
			return
		}
		localkeys = append(localkeys, element)
		remotekeys = append(remotekeys, element)
	}

	for i := 0; i < numDifferences; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			t.Error("Could not get random bytes for set element")
			return
		}
		// Add to a set at random
		diffSet := &localkeys
		if rand.Intn(2) == 0 {
			diffSet = &remotekeys
		}
		*diffSet = append(*diffSet, element)
	}

	fmt.Printf("Size of local set : %v\n", len(localkeys))
	fmt.Printf("Size of remote set: %v\n", len(remotekeys))
	fmt.Printf("Number of difference: %v\n", numDifferences)

	//calculate the depth required to contain all values
	var count int
	if len(localkeys) > len(remotekeys) {
		count = len(localkeys)
	} else {
		count = len(remotekeys)
	}

	depth := int(math.Ceil(math.Log2(float64(count))))
	fmt.Printf("Depth: %v\n", depth)

	localstrata := NewStrata(cellSize, keysize, depth)
	remotestrata := NewStrata(cellSize, keysize, depth)

	localstrata.Populate(localkeys)
	remotestrata.Populate(remotekeys)

	diffloc := localstrata.Estimate(remotestrata)
	//diffrem := remotestrata.Estimate(localstrata)
	fmt.Printf("Real Diff: %v, Estimated: %v \n", numDifferences, diffloc)
	fmt.Printf("Error: %v%%\n", 100*math.Abs(1.0-float64(diffloc)/float64(numDifferences)))
	//fmt.Printf("Real Diff: %v, Estimated: %v \n", numDifferences, diffrem)

}
