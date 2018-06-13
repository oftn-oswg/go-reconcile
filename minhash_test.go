package reconcile

import (
	"math"
	"testing"
)

func TestMinHash(t *testing.T) {
	numElements := 100
	numDifferences := 80
	hashCount := 100
	keysize := 32
	local := NewMinHash(hashCount)
	remote := NewMinHash(hashCount)

	// Prepare elements
	localElements, remoteElements, _, _, _ := MakeTestSets(keysize, numElements, numDifferences)

	for _, element := range localElements {
		local.Add(element)
	}
	for _, element := range remoteElements {
		remote.Add(element)
	}

	count, err := local.Estimate(remote)
	if err != nil {
		t.Error(err)
		return
	}

	jaccard, err := local.Similarity(remote)
	if err != nil {
		t.Error(err)
		return
	}

	// A bound for expected error for the Jaccard similarity coefficient J is O(1 / sqrt(hashCount))
	jaccardEpsilon := -1.0 / math.Sqrt(float64(hashCount))

	jaccardMin := jaccard - jaccardEpsilon
	if jaccardMin < 0 {
		jaccardMin = 0
	}
	jaccardMax := jaccard + jaccardEpsilon
	if jaccardMax > 1 {
		jaccardMax = 1
	}

	countSum := float64(len(localElements) + len(remoteElements))
	countMin := int(countSum * (1.0 - jaccardMin) / (1.0 + jaccardMin))
	countMax := int(countSum * (1.0 - jaccardMax) / (1.0 + jaccardMax))

	title := "MinHash"
	if countMin > count || count > countMax {
		t.Error(
			"For", title, "test",
			"expected between", countMin, "and", countMax,
			"got", count)
		return
	}

	t.Log(
		"Success for", title, "test",
		"expected between", countMin, "and", countMax,
		"got", count)
}
