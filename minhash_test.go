package reconcile

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestMinHash(t *testing.T) {
	numElements := 1000
	numDifferences := 1000
	var hashCount uint32 = 100
	keysize := 32

	localkeys := [][]byte{}
	remotekeys := [][]byte{}

	for i := 0; i < numElements; i++ {
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

	sigA := GetMinHashSignature(localkeys, hashCount)
	sigB := GetMinHashSignature(remotekeys, hashCount)

	diff := MinHashDifference(sigA, sigB)

	fmt.Printf("MinHash Diff %v out of %v\n", diff, int(hashCount)*keysize*8)
	//fmt.Printf("%v%%\n", diff, int(hashCount)*keysize*8)

	fmt.Printf("End MinHash \n")
}
