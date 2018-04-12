package reconcile

//Inputs:
//-A local set
//-A remote strata estimator
//-A remote MiHash signature

//Estimates the difference between the local and remote set
//Builds an IBF of size estimated * factor
//Returns two sets of what is missing from either data sets

//Usage steps:
//NewSetReconciler(set)
//Estimate size of difference
//Get Missing Elements

//Size of difference involves partitioning keys into n levels
//where size of leveln = len(set) / (2^n)
//Large Partitions are handed to minhash estimator
//Level 1 - 2
//Strata estimator operates on 3..

//1: Both parties
//  Generate Two Signatures
//  level1-2 minhash
//  further strata
//filter := NewIBF(cells, keysize)

//create the reconciler
//exchange set sizes and agree on minimum size
//TEMPORARY!!
//use local set to build minhash and strata estimator data structure
//exchange with other party
//estimate difference size
//build ibf
//exchange ibfs
//get difference

type Reconcile struct {

}

func NewSetReconciler(keys [][]byte) {

    IBFset := make([]*IBF, )
}
