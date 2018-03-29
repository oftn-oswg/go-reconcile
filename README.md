Set reconcilation functions

Required operation:
Both parties generate a Filter
Both parties exchange filters
(A) Local party
(B) Remote party
Subtract A-B for what's missing from B
Subtract B-A for what's mising from A
Look for pure cells check Hash(idSum) == hashSum -> purelist index
for each in purelist find id
&	add id to difflist
	rehash id to find all other ibf cells containing it
	remove id from all those cells; xor id, xor hash, decrement count
look for pure cells goto & until all cells zero
if no pure cells remainig and not zero then failed
if failed goto fallback Method

Input:
- array of file contents hashes from local database
- IBF from remote
