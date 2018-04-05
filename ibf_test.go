package reconcile

import (
	"bytes"
	"encoding/hex"
	"log"
	"math/rand"
	"testing"
)

func elementName(key []byte) string {
	return "'" + hex.EncodeToString(key) + "'"
}

func makeRandomElements(count, keysize int) [][]byte {
	elements := make([][]byte, count)
	for i := 0; i < count; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			log.Panicln("Could not get random bytes for set element")
		}
		elements[i] = element
	}
	return elements
}

func containsElement(list [][]byte, element []byte) bool {
	for _, listElement := range list {
		if bytes.Equal(listElement, element) {
			return true
		}
	}
	return false
}

func TestReconcile(t *testing.T) {
	keysize := 32

	for base := 1; base <= 9; base += 4 {
		for diffs := 1; diffs <= 8; diffs++ {
			cells := 2 + diffs*4
			t.Logf("Testing with %d common and %d different elements in %d cells", base, diffs, cells)

			// Prepare elements
			common := makeRandomElements(base, keysize)
			partition := rand.Intn(diffs + 1)
			elementsAunique := makeRandomElements(partition, keysize)
			elementsBunique := makeRandomElements(diffs-partition, keysize)
			elementsA := append(append(make([][]byte, 0, base+len(elementsAunique)), common...), elementsAunique...)
			elementsB := append(append(make([][]byte, 0, base+len(elementsBunique)), common...), elementsBunique...)

			for _, element := range common {
				t.Logf("Letting %s ∈ A ∩ B", elementName(element))
			}
			for _, element := range elementsAunique {
				t.Logf("Letting %s ∈ A", elementName(element))
			}
			for _, element := range elementsBunique {
				t.Logf("Letting %s ∈ B", elementName(element))
			}

			// Construct filters
			filterA := NewIBF(cells, keysize)
			filterB := NewIBF(cells, keysize)
			for _, element := range elementsA {
				if err := filterA.Add(element); err != nil {
					t.Error(err)
				}
			}
			for _, element := range elementsB {
				if err := filterB.Add(element); err != nil {
					t.Error(err)
				}
			}

			// Perform decoding
			filterA.Subtract(filterB)
			local, remote, complete := filterA.Decode()

			t.Logf("We have %s deduction",
				(map[bool]string{true: "a complete", false: "an incomplete"})[complete])

			// Expect local ⊆ A − B
			for _, element := range local {
				if containsElement(elementsAunique, element) {
					t.Logf("Local's %s ∈ A − B", elementName(element))
				} else {
					t.Errorf("Local's %s ∉ A − B", elementName(element))
				}
			}

			// Expect remote ⊆ B − A
			for _, element := range remote {
				if containsElement(elementsBunique, element) {
					t.Logf("Remote's %s ∈ B − A", elementName(element))
				} else {
					t.Errorf("Remote's %s ∉ B − A", elementName(element))
				}
			}

			if complete {
				// Expect A − B ⊆ local
				for _, element := range elementsAunique {
					if containsElement(local, element) {
						t.Logf("A's %s ∈ local", elementName(element))
					} else {
						t.Errorf("A's %s ∉ local", elementName(element))
					}
				}
				// Expect B − A ⊆ remote
				for _, element := range elementsBunique {
					if containsElement(remote, element) {
						t.Logf("B's %s ∈ remote", elementName(element))
					} else {
						t.Errorf("B's %s ∉ remote", elementName(element))
					}
				}
			}

		}
	}
}
