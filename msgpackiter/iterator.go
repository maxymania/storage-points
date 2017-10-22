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


// High performance MSGPACK parser.
package msgpackiter

import "fmt"

type Iterator struct{
	saved []int
	count int
	buffer []byte
}
func (i *Iterator) Reset(buffer []byte) *Iterator {
	i.buffer = buffer
	i.count = 0
	i.saved = i.saved[:0]
	return i
}
func (i *Iterator) WhatIsNext() ValueType {
	return PeekValue(i.buffer)
}
func (i *Iterator) Skip() {
	switch PeekValue(i.buffer) {
	case ArrayType,MapType:
		off,lng := ReadArrayLength(i.buffer)
		i.buffer = i.buffer[off:]
		for lng>0 {
			lng--
			i.Skip()
		}
		return
	}
	sl := ScalarLength(i.buffer)
	if len(i.buffer)<=sl { i.buffer = nil } else { i.buffer = i.buffer[sl:] }
}
func (i *Iterator) SkipRest() {
	for i.ArrayNext() {
		i.Skip()
	}
	lis := len(i.saved)-1
	i.saved,i.count = i.saved[:lis],i.saved[lis]
}
func (i *Iterator) ArrayNext() bool {
	if i.count<=0 { return false }
	i.count--
	return true
}
// This function allocates
func (i *Iterator) MapNext() (string,bool) {
	if i.count<=0 { return "",false }
	if (i.count&1)==0 {
		s := i.ReadMapKey()
		i.count-=2
		return s,true
	}
	return "",false
}
func (i *Iterator) EndMap() { i.SkipRest() }
func (i *Iterator) EndArray() { i.SkipRest() }
func (i *Iterator) BeginMap() bool { return i.BeginArray() }
func (i *Iterator) BeginArray() bool {
	off,lng := ReadArrayLength(i.buffer)
	if off==0 { return false }
	i.saved = append(i.saved,i.count)
	i.count = lng
	i.buffer = i.buffer[off:]
	return true
}
func (i *Iterator) ReadInt() int64 {
	l := ScalarLength(i.buffer)
	if len(i.buffer)<l { return 0 }
	v,l := ReadInt(i.buffer)
	i.buffer = i.buffer[l:]
	return v
}
func (i *Iterator) ReadUint() uint64 {
	l := ScalarLength(i.buffer)
	if len(i.buffer)<l { return 0 }
	v,l := ReadUint(i.buffer)
	i.buffer = i.buffer[l:]
	return v
}
func (i *Iterator) ReadFloat() float64 {
	l := ScalarLength(i.buffer)
	if len(i.buffer)<l { return 0 }
	v,l := ReadFloat(i.buffer)
	i.buffer = i.buffer[l:]
	return v
}
func (i *Iterator) ReadSlice() []byte {
	b,l := StringRangeLength(i.buffer)
	if len(i.buffer)<l { return nil }
	bts := i.buffer[b:l]
	i.buffer = i.buffer[l:]
	return bts
}
// This function allocates
func (i *Iterator) ReadString() string {
	b,l := StringRangeLength(i.buffer)
	if len(i.buffer)<l { return "" }
	bts := i.buffer[b:l]
	i.buffer = i.buffer[l:]
	return string(bts)
}
// This function allocates
func (i *Iterator) ReadMapKey() (ret string) {
	l := ScalarLength(i.buffer)
	if len(i.buffer)<l { return }
	ibuf := i.buffer[l:]
	switch PeekValue(i.buffer) {
	case StringType,BinaryType:
		b,e := StringRangeLength(i.buffer)
		ret = string(i.buffer[b:e])
	case IntType:
		ii,_ := ReadInt(i.buffer)
		ret = fmt.Sprint(ii)
	case FloatType:
		f,_ := ReadFloat(i.buffer)
		ret = fmt.Sprintf("%f",f)
	case NilType:
		ret = "<nil>"
	case BoolType:
		bb,_ := ReadBool(i.buffer)
		ret = fmt.Sprint(bb)
	case ArrayType,MapType:
		i.Skip()
		return 
	}
	i.buffer = ibuf
	return
}
func (i *Iterator) ReadBool(buf []byte) bool {
	b,l := ReadBool(i.buffer)
	i.buffer = i.buffer[l:]
	return b
}


