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
import "sync"
import "bytes"
import "hash/crc64"
import "math/rand"
import "github.com/steveyen/gtreap"
import "sort"

var ecmaTable = crc64.MakeTable(crc64.ECMA)

func ecmaCrc(p []byte,crc uint64) uint64{
	crc = ^crc
	for _, v := range p {
		crc = ecmaTable[byte(crc)^v] ^ (crc >> 8)
	}
	return ^crc
}
func hamming(b uint64) (i int) {
	for b!=0 {
		b &= b-1
		i++
	}
	return
}

func bcomp(a, b interface{}) int { return bytes.Compare(a.(Partition).Name,b.(Partition).Name) }

var masterTreap = gtreap.NewTreap(bcomp)

type Partition struct{
	Name  []byte
	Crc64 uint64
}

type reselem struct{
	name []byte
	hw int
}

type Partitioner struct{
	wl sync.Mutex
	treap *gtreap.Treap
	count int
}
func (p *Partitioner) Insert(name []byte) {
	p.wl.Lock(); defer p.wl.Unlock()
	var i gtreap.Item
	i = Partition{name,ecmaCrc(name,0)}
	treap := p.treap
	if treap==nil { treap = masterTreap }
	if treap.Get(i)!=nil { return } // We have him!
	p.count++
	p.treap = treap.Upsert(i,rand.Int())
}
func (p *Partitioner) Remove(name []byte) {
	p.wl.Lock(); defer p.wl.Unlock()
	var i gtreap.Item
	i = Partition{name,ecmaCrc(name,0)}
	if p.treap==nil { return }
	if p.treap.Get(i)!=nil { return } // We have him!
	p.count--
	p.treap = p.treap.Delete(i)
}
func (p *Partitioner) RingRange(id []byte) [][]byte {
	// TODO: This function does have at least two big allocations.
	
	hash := ecmaCrc(id,0)
	t,c := p.treap,p.count
	start := t.Min()
	array := make([]reselem,0,c+10)
	t.VisitAscend(start,func(i gtreap.Item) bool{
		p := i.(Partition)
		array = append(array,reselem{p.Name,hamming(p.Crc64^hash)})
		return true
	})
	sort.SliceStable(array,func(i,j int)bool { return array[i].hw<array[j].hw })
	ra := make([][]byte,len(array))
	for i,v := range array { ra[i]=v.name }
	return ra
}
// []SplitCollumn
func (p *Partitioner) ringRange(id []byte,array []reselem) []reselem {
	// TODO: This function does have at least two big allocations.
	
	hash := ecmaCrc(id,0)
	t,_ := p.treap,p.count
	start := t.Min()
	array  = array[:0]
	t.VisitAscend(start,func(i gtreap.Item) bool{
		p := i.(Partition)
		array = append(array,reselem{p.Name,hamming(p.Crc64^hash)})
		return true
	})
	sort.SliceStable(array,func(i,j int)bool { return array[i].hw<array[j].hw })
	return array
}
func (p *Partitioner) RingRanges(scs []SplitCollumn,sizePerRange int){
	array := make([]reselem,p.count)
	for i := range scs {
		array = p.ringRange(scs[i].Value,array)
		n := len(array)
		if n>sizePerRange { n = sizePerRange }
		ra := make([][]byte,n)
		for j := range ra { ra[j]=array[j].name }
		sort.Slice(ra,func(k,l int) bool { return bytes.Compare(ra[k],ra[l])<0 })
		scs[i].Spaces = ra
	}
}


