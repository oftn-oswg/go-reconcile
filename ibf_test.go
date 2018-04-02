package reconcile

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"testing"
)

func elementName(key []byte) string {
	return "'" + hex.EncodeToString(key) + "'"
}

func TestReconcile(t *testing.T) {
	numDifferences := 1
	numBaseElements := 8
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

	for numDifferences < 8 {
		t.Logf("Testing sets with %d differences and %d similarities", numDifferences, numBaseElements)

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

		cells := 2 + numDifferences*4
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

			t.Logf("Letting %s ∈ A ∩ B", elementName(element))
		}

		for _, element := range diffSetA {
			err := filterA.Add(element)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("Letting %s ∈ A", elementName(element))
		}

		for _, element := range diffSetB {
			err := filterB.Add(element)
			if err != nil {
				t.Error(err)
				return
			}
			t.Logf("Letting %s ∈ B", elementName(element))
		}

		// Our sets are created.
		// TODO: Serialize first
		filterA.Subtract(filterB)
		AdiffB, BdiffA, err := filterA.Decode()

		if err != nil {
			t.Errorf("Could not decode all differences")
		}

		for _, element := range AdiffB {
			t.Logf("Found %s ∈ A − B\n", elementName(element))
		}

		for _, element := range BdiffA {
			t.Logf("Found %s ∈ B − A\n", elementName(element))
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
				t.Errorf("%s ∉ A − B", elementName(AdiffBelement))
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
				t.Errorf("%s ∉ B − A", elementName(BdiffAelement))
			}
		}

		numDifferences++
	}
}
