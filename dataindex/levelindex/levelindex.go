/*
Copyright (c) 2017 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package levelindex

//import "encoding/binary"
import "github.com/maxymania/storage-points/dataindex"
import "github.com/syndtr/goleveldb/leveldb"
//import "github.com/nu7hatch/gouuid"
import "github.com/maxymania/storage-points/msgpackiter"

type IndexDB struct{
	DB *leveldb.DB
}

func (i *IndexDB) Insert(id, data []byte) error {
	
	iter := new(msgpackiter.Iterator).Reset(data)
	if !iter.BeginArray() { return dataindex.EMalformedValue }
	
	tr,err := i.DB.OpenTransaction()
	if err!=nil { return err }
	
	key := append(append(make([]byte,0,256),'>'),id...)
	
	invk := make([]byte,256)
	for count := 16 ; count>0 ; count-- {
		temp,_ := tr.Get(key,nil)
		if len(temp)>0 { return dataindex.EDuplicateKey }
		err = tr.Put(key,data,nil)
		if err!=nil { return err }
		for iter.ArrayNext() {
			invk = append(invk[:0],'$')
			invk = append(invk,iter.ReadSlice()...)
			invk = append(invk,':')
			iter.ArrayNext()
			invk = append(invk,iter.ReadSlice()...)
			err = tr.Put(invk,key,nil)
			if err!=nil { return err }
		}
		if tr.Commit()==nil { return nil }
		tr.Discard()
		tr,err = i.DB.OpenTransaction()
		if err!=nil { return err }
	}
	
	return dataindex.ECommitFailed
}

