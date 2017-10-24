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


package loader

import "github.com/maxymania/storage-points/storage"
import "github.com/maxymania/storage-points/guido"
import "errors"

var ENoSuchBackend = errors.New("No such backend")
var Backends = make(map[string]storage.KVP_Factory)

type Partition struct{
	Name string
	KVP  storage.KeyValuePartition
}
func Load(name, path string) (*Partition,error) {
	bak,ok := Backends[name]
	if !ok { return nil,ENoSuchBackend }
	return LoadCustom(bak,path)
}
func LoadCustom(backend storage.KVP_Factory, path string) (*Partition,error) {
	bak := backend
	id,err := guido.GetUID(path)
	if err!=nil { return nil,err }
	kvp,err := bak.OpenKVP(path)
	if err!=nil { return nil,err }
	
	p := new(Partition)
	p.Name = id.String()
	p.KVP = kvp
	return p,nil
}

