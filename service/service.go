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


package service

import "github.com/maxymania/storage-points/storage"
import "github.com/valyala/fasthttp"
import "bytes"
import "fmt"

// '/c9c935b3-0a1c-40c1-75a6-61df8dda3ec4/ObjectName'
// '/all/ObjectName'
// 

func split(b []byte,c byte) (a1,a2 []byte) {
	i := bytes.IndexByte(b,c)
	if i<0 { return b,nil }
	return b[:i],b[i+1:]
}


type Partition struct{
	KVP storage.KeyValuePartition
}

type ServiceHandler struct{
	Partitions map[string]Partition
}
func (s *ServiceHandler) Init() {
	s.Partitions = make(map[string]Partition)
}
func (s *ServiceHandler) Handle(ctx *fasthttp.RequestCtx){
	_,path := split(ctx.Path(),'/')
	part,path := split(path,'/')
	sub,path := split(path,'/')
	//if i<0 { ctx.Error("Unsupported path", fasthttp.StatusNotFound) ; return }
	fmt.Printf("%q /%q/%q/%q\n",ctx.Method(),part,sub,path)
	switch string(part) {
	case "all":
		switch string(ctx.Method()) {
		case "GET":
			for n,p := range s.Partitions {
				err := p.KVP.Get(sub,ctx)
				if err!=nil {
					continue
				}
				ctx.Response.Header.Set("Partition", n)
				return
			}
			ctx.Error("Not found\n", fasthttp.StatusNotFound)
			return
		}
	}
	if partition,ok := s.Partitions[string(part)] ; ok {
		
		
		switch string(ctx.Method()) {
		case "GET":
			{
				err := partition.KVP.Get(sub,ctx)
				if err!=nil { ctx.Error("Not found\n", fasthttp.StatusNotFound) }
				return
			}
		case "PUT":
			{
				err := partition.KVP.Put(sub,ctx.Request.Body())
				if err!=nil { ctx.Error("Not found\n", fasthttp.StatusNotFound) }
				ctx.Error("OK\n", 200)
				return
			}
		}
	}
	
	ctx.Error("Unsupported path\n", fasthttp.StatusNotImplemented) // StatusNotFound
}


