package reconcile

import "fmt"

//Inverse Bloom Filter

//Specifics
//Number of hash functions
//	3
//Filter size is proportional to size of difference

//Current State:
//Uses PeerResourceDigest to identify files (quite large 32bytes)
//Uses hopelessly basic hash functions for test
//No calculation of set size for test.
//	Currently assumes set is less than 65536

type IBFnode struct {
	idSum   int
	hashSum int
	idCount int //16 enough?
}

type IBF struct {
	table []IBFnode
	size  int
}

func Generate(ids []int, size int) []IBFnode {
	table := make([]IBFnode, size)

	for _, id := range ids {
		var node IBFnode
		node = table[ModHash(id, size)]
		node.idSum ^= id
		node.hashSum ^= ModHash(id, size)
		node.idCount++
		table[ModHash(id, size)] = node
		node = table[DivHash(id, size)]
		node.idSum ^= id
		node.hashSum ^= ModHash(id, size)
		node.idCount++
		table[DivHash(id, size)] = node
		node = table[InvHash(id, size)]
		node.idSum ^= id
		node.hashSum ^= ModHash(id, size)
		node.idCount++
		table[InvHash(id, size)] = node
	}
	return table
}

func Reconcile(local []int, remote []int) (bool, l []int, r []int) {
	if len(local) != len(remote) {
		fmt.Println("Filters are not the same size!")
	}

	f := Subtract(remote, local) //remote - local for what files local doesn't have

}

//poor made up hash functions
func ModHash(id int, size int) int { return (id % size) }
func DivHash(id int, size int) int { return ((id >> 1) / 7 % size) }
func InvHash(id int, size int) int { return ((id ^ 0x55555555) % size) }
