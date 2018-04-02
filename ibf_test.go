package reconcile

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestReconcile(t *testing.T) {
	numDifferences := 1
	numBaseElements := 0
	keysize := 32

	baseSet := [][]byte{}
	for i := 0; i < numBaseElements; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			t.Error("Could not get random bytes for set element")
			return
		}
		baseSet = append(baseSet, element)
	}

	for numDifferences < 3 {
		diffSetA := [][]byte{}
		diffSetB := [][]byte{}
		for i := 0; i < numDifferences; i++ {
			element := make([]byte, keysize)
			_, err := rand.Read(element)
			if err != nil {
				t.Error("Could not get random bytes for set element")
				return
			}
			// Add to a set at random
			diffSet := &diffSetA
			if rand.Intn(2) == 0 {
				diffSet = &diffSetB
			}
			*diffSet = append(*diffSet, element)
		}

		cells := numDifferences * 4
		filterA := New(cells, keysize)
		filterB := New(cells, keysize)

		for _, element := range baseSet {
			// Add to both sets
			err := filterA.Add(element)
			if err != nil {
				t.Error(err)
				return
			}

			err = filterB.Add(element)
			if err != nil {
				t.Error(err)
				return
			}

			log.Printf("Adding %s to sets A and B", hex.EncodeToString(element))
		}

		for _, element := range diffSetA {
			err := filterA.Add(element)
			if err != nil {
				t.Error(err)
				return
			}
			log.Printf("Adding %s to set A", hex.EncodeToString(element))
		}

		for _, element := range diffSetB {
			err := filterB.Add(element)
			if err != nil {
				t.Error(err)
				return
			}
			log.Printf("Adding %s to set B", hex.EncodeToString(element))
		}

		// Our sets are created.
		// TODO: Serialize first
		filterA.Subtract(filterB)
		jsonA, _ := json.Marshal(filterA)
		fmt.Printf("%s\n\n", string(jsonA))

		AdiffB, BdiffA, err := filterA.Decode()
		if err != nil {
			t.Errorf("For %d differences, could not decode", numDifferences)
		}

		for _, AdiffBelement := range AdiffB {
			// Fail if AdiffBelement is not in A or is in B
			inA := false
			inB := false
			for _, element := range diffSetA {
				// Look in setA
				if bytes.Equal(AdiffBelement, element) {
					inA = true
					break
				}
			}

			for _, element := range diffSetB {
				// Look in setA
				if bytes.Equal(AdiffBelement, element) {
					inB = true
					break
				}
			}

			if !inA || inB {
				t.Errorf("Element %s is not in set A, or is in B", hex.EncodeToString(AdiffBelement))
			}
		}

		for _, BdiffAelement := range BdiffA {
			// Fail if BdiffAelement is not in B or is in A
			inA := false
			inB := false
			for _, element := range diffSetA {
				// Look in setA
				if bytes.Equal(BdiffAelement, element) {
					inA = true
					break
				}
			}

			for _, element := range diffSetB {
				// Look in setA
				if bytes.Equal(BdiffAelement, element) {
					inB = true
					break
				}
			}

			if inA || !inB {
				t.Errorf("Element %s is not in set B, or is in A", hex.EncodeToString(BdiffAelement))
			}
		}

		numDifferences++
	}
}
