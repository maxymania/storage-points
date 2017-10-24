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


package storage

import "io"
import "github.com/syndtr/goleveldb/leveldb"
import . "github.com/maxymania/storage-points/storage"
import "github.com/maxymania/storage-points/storage/loader"
import "path/filepath"
import "os"

type SimplePartition struct{
	DB *leveldb.DB
}
func (s *SimplePartition) Put(id, value []byte) error {
	if len(value)==0 {
		return s.DB.Delete(id,nil)
	}
	return s.DB.Put(id,value,nil)
}
func (s *SimplePartition) Get(id []byte, dest io.Writer) error {
	dbuf,err := s.DB.Get(id,nil)
	if err!=nil {
		if err==leveldb.ErrNotFound { err = ENotFound }
		return err
	}
	_,err = dest.Write(dbuf)
	return err
}
type SimplePartitionFactory struct{}
func (s SimplePartitionFactory) OpenKVP(path string) (KeyValuePartition,error) {
	ldb := filepath.Join(path,"leveldb")
	os.Mkdir(ldb, 0700)
	db,err := leveldb.OpenFile(ldb,nil)
	if err!=nil { return nil,err }
	return &SimplePartition{db},nil
}

func init(){
	loader.Backends["leveldb"] = SimplePartitionFactory{}
}

