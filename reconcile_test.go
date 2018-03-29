package reconcile

import (
	"fmt"
	"testing"
)

type TestType struct {
	local  []int
	remote []int
	diff   []int
}

func TestReconcile(t *testing.T) {
	//What is missing from local?

	tests := []struct {
		local  []int
		remote []int
		diff   []int
	}{
		{
			[]int{765, 298, 594, 2, 2391, 239090, 102, 9, 3},
			[]int{223, 765, 298, 594, 2, 19439, 2391, 239090, 102, 9, 3},
			[]int{223, 19439},
		},
		{
			[]int{223, 765, 298, 594, 2, 19439, 2391, 239090, 102, 9},
			[]int{223, 765, 298, 594, 19439, 2391, 102, 9, 3},
			[]int{3},
		},
	}

	for _, test := range tests {
		loc := Generate(test.local, 11)
		rem := Generate(test.remote, 11)

		diff := Reconcile(loc, rem)

		if diff != test.diff {
			t.Error(
				"Try again, please insert coin.")
		}
	}
}

func TestGenerate(t *testing.T) {
	ids := [5]int{0, 1, 2, 3, 4}
	table := Generate(ids[:], 5)
	node := table[3]

	if node.idCount != 1 {
		fmt.Println(node.idCount)
		t.Error(
			"Try again, please insert coin.")
	}

	node = table[0]

	if node.idCount != 7 {
		fmt.Println(node.idCount)
		t.Error(
			"Try again, please insert coin.")
	}
}

func TestHash(t *testing.T) {
	if ModHash(3, 5) != 3 {
		t.Error(
			"ModHashFail.")
	}
	if DivHash(3, 5) != 0 {
		t.Error(
			"DivHashFail.")
	}
	if InvHash(3, 5) != 1 {
		t.Error(
			"InvHashFail.")
	}
}
