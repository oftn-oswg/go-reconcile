package reconcile

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

//Functional Steps:
//Both parties start the transaction by informing each other of their set sizes.
//Then Generate strata signatures and exchange in JSON.
//Use this to estimate the size of the difference
//Build IBF of this size and exchange signatures in JSON
//Calculate the difference

//Creates two set of keys
func NewTestSets(keysize, match, uniquea, uniqueb int) (a [][]byte, b [][]byte) {
	a = make([][]byte, match+uniquea)
	b = make([][]byte, match+uniqueb)

	for i := 0; i < match; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			log.Panicln("Could not get random bytes for set element")
		}
		a[i] = element
		b[i] = element
	}

	for i := match; i < match+uniquea; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			log.Panicln("Could not get random bytes for set element")
		}
		a[i] = element
	}

	for i := match; i < match+uniqueb; i++ {
		element := make([]byte, keysize)
		_, err := rand.Read(element)
		if err != nil {
			log.Panicln("Could not get random bytes for set element")
		}
		b[i] = element
	}

	return
}

func TestReconcile(t *testing.T) {
	keysize := 32
	matchingcount := 50
	uniquea := 40
	uniqueb := 20
	localset, remoteset := NewTestSets(keysize, matchingcount, uniquea, uniqueb)

	local := NewReconcile(localset, len(remoteset))
	remote := NewReconcile(remoteset, len(localset))

	//Exchange JSON strata signatures
	locdiffestimator, _ := local.GetDifferenceSizeEstimator()
	remdiffestimator, _ := remote.GetDifferenceSizeEstimator()
	locdiffsize, _ := local.EstimateDifferenceSize(locdiffestimator)
	remdiffsize, _ := remote.EstimateDifferenceSize(remdiffestimator)

	if locdiffsize != remdiffsize {
		t.Error("Difference size error")
	}

	//Exchange IBF signatures and get difference
	localsignature, _ := local.GetIBFSignature(locdiffsize)
	remotesignature, _ := remote.GetIBFSignature(remdiffsize)
	loca, locb, _ := local.GetDifference(locdiffsize, remotesignature)
	rema, remb, _ := remote.GetDifference(remdiffsize, localsignature)

	fmt.Println(loca)
	fmt.Println(locb)
	fmt.Println(rema)
	fmt.Println(remb)
}
