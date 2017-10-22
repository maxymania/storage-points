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

type KeyValuePartition interface{
	Put(id, value []byte) error
	Get(id []byte, dest io.Writer) error
}
type SimplePartition struct{
	DB *leveldb.DB
}
func (s *SimplePartition) Put(id, value []byte) error {
	return s.DB.Put(id,value,nil)
}
func (s *SimplePartition) Get(id []byte, dest io.Writer) error {
	dbuf,err := s.DB.Get(id,nil)
	if err!=nil { return err }
	_,err = dest.Write(dbuf)
	return err
}

