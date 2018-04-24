package reconcile

import (
	"math"
)

//Create reconciler with local keys knowing the remote set size
//Generate strata signature and transmit
//Receive remote strata signature, estimate size
//Build IBF of 'size' and transmit signature
//Receive remote IBF signature and calculate difference

type Reconcile struct {
	Keyset    [][]byte
	Estimator *Strata
	Depth     int
}

//Creates a set reconciler and populates a size estimator with all local keys
func NewReconcile(keys [][]byte, remotesetsize int) *Reconcile {
	//Get the required depth
	var depth int
	if remotesetsize > len(keys) {
		depth = int(math.Ceil(math.Log2(float64(remotesetsize))))
	} else {
		depth = int(math.Ceil(math.Log2(float64(len(keys)))))
	}

	//Create and populate and return the local IBF
	estimator := NewStrata(80, len(keys[0]), depth)
	estimator.Populate(keys)

	return &Reconcile{keys, estimator, depth}
}

func (r *Reconcile) GetDifferenceSizeEstimator() ([]byte, error) {
	json, err := r.Estimator.MarshalStrataJSON()
	return json, err
}

//Takes JSON estimator data from remote and estimates size of difference
func (r *Reconcile) EstimateDifferenceSize(data []byte) (int, error) {
	remote := NewStrata(80, len(r.Keyset[0]), r.Depth)
	err := remote.UnmarshalStrataJSON(data)
	return r.Estimator.Estimate(remote), err
}

//Generates signature of ibf dataset
//Must be called after estimating difference size
func (r *Reconcile) GetIBFSignature(size int) ([]byte, error) {
	ibf := NewIBF(size, len(r.Keyset[0]))
	for _, key := range r.Keyset {
		ibf.Add(key)
	}
	return ibf.MarshalJSON()
}

func (r *Reconcile) GetDifference(size int, remotesignature []byte) (a [][]byte, b [][]byte, ok bool) {
	ibf := NewIBF(size, len(r.Keyset[0]))
	for _, key := range r.Keyset {
		ibf.Add(key)
	}
	remoteibf := NewIBF(size, len(r.Keyset[0]))
	remoteibf.UnmarshalJSON(remotesignature)
	ibf.Subtract(remoteibf)
	return ibf.Decode()
}
