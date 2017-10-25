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


package dataindex
// github.com/maxymania/storage-points/dataindex

import mpacki "github.com/maxymania/storage-points/msgpackiter"
//import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/golibs/buffer"
import "fmt"

type SplitCollumn struct {
	Value  []byte
	Spaces [][]byte
	Alloc  *[]byte
}
func (s SplitCollumn) String() string {
	return fmt.Sprintf("{%q %q %p}",s.Value,s.Spaces,s.Alloc)
}

func SplittenMessage(msg []byte) []SplitCollumn {
	iter := new(mpacki.Iterator).Reset(msg)
	iter.Skip()
	
	cols := []SplitCollumn{}
	
	if !iter.BeginMap() { return nil }
	for iter.ArrayNext() {
		sls := iter.ReadSlice()
		iter.ArrayNext()
		if len(sls)==0 || sls[0]!='$' {
			iter.Skip()
			continue
		}
		switch iter.WhatIsNext() {
		case mpacki.StringType,mpacki.BinaryType:
			val := iter.ReadSlice()
			rbuf := buffer.Get(len(sls)+len(val)+1)
			buf := (*rbuf)[:0]
			buf = append(buf,sls...)
			buf = append(buf,':')
			buf = append(buf,val...)
			cols = append(cols,SplitCollumn{Value:buf,Alloc:rbuf})
		case mpacki.ArrayType:
			iter.BeginArray()
			for iter.ArrayNext() {
				val := iter.ReadSlice()
				rbuf := buffer.Get(len(sls)+len(val)+1)
				buf := (*rbuf)[:0]
				buf = append(buf,sls...)
				buf = append(buf,':')
				buf = append(buf,val...)
				cols = append(cols,SplitCollumn{Value:buf,Alloc:rbuf})
			}
			iter.SkipRest()
		default: iter.Skip()
		}
	}
	return cols
}


