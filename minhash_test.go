package reconcile

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestMinHash(t *testing.T) {
	numElements := 100
	numDifferences := 80
	hashCount := 100
	keysize := 32
	local := NewMinHash(hashCount, keysize)
	remote := NewMinHash(hashCount, keysize)

	for i := 0; i < numElements; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			t.Error("Could not get random bytes for set element")
			return
		}
		local.Add(element)
		remote.Add(element)
	}

	for i := 0; i < numDifferences; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			t.Error("Could not get random bytes for set element")
			return
		}
		// Add to a set at random
		diffSet := local
		if rand.Intn(2) == 0 {
			diffSet = remote
		}
		diffSet.Add(element)
	}

	diff := local.Difference(remote)

	fmt.Printf("MinHash Diff %v vs actual: %v\n", diff, numDifferences)

	fmt.Printf("End MinHash \n")
}
