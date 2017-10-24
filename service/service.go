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
import "github.com/maxymania/storage-points/storage/loader"
import "github.com/valyala/fasthttp"
import "bytes"
import "sync"
import "github.com/json-iterator/go"
import "time"

// '/c9c935b3-0a1c-40c1-75a6-61df8dda3ec4/ObjectName'
// '/all/ObjectName'
// 

func split(b []byte,c byte) (a1,a2 []byte) {
	i := bytes.IndexByte(b,c)
	if i<0 { return b,nil }
	return b[:i],b[i+1:]
}

type comboLock struct{
	sync.WaitGroup
	sync.Mutex
}

type Peer struct{
	Name       string
	Client     PeerClient
	Partitions []string
}

type ServiceHandler struct{
	Partitions map[string]loader.Partition
	
	peersLock sync.RWMutex
	peers     map[string]*Peer
	peerParts map[string]string
}
func (s *ServiceHandler) Init() {
	s.Partitions = make(map[string]loader.Partition)
	s.peers      = make(map[string]*Peer)
	s.peerParts  = make(map[string]string)
}
func (s *ServiceHandler) Add(ld *loader.Partition) {
	s.Partitions[ld.Name] = *ld
}
func (s *ServiceHandler) AddOrUpdatePeer(peer *Peer) {
	s.peersLock.Lock(); defer s.peersLock.Unlock()
	n := peer.Name
	for k,v := range s.peerParts { if n==v { delete(s.peerParts,k) } }
	s.peers[n] = peer
	for _,k := range peer.Partitions { s.peerParts[k] = n }
}
func (s *ServiceHandler) lookupPartitionPeer(part []byte) *Peer {
	s.peersLock.RLock(); defer s.peersLock.RUnlock()
	n,ok := s.peerParts[string(part)]
	if !ok { return nil }
	return s.peers[n]
}

func (s *ServiceHandler) Handle(ctx *fasthttp.RequestCtx){
	_,path := split(ctx.Path(),'/')
	part,path := split(path,'/')
	sub,path := split(path,'/')
	
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
			ctx.Response.Header.Set("Error-404", "key")
			
			if string(ctx.Request.Header.Peek("No-Hops"))!="True" {
				// At this point The  "No-Hops: True" -Header is not set.
				
				// We got to loop trough all nodes
				req := fasthttp.AcquireRequest()
				ctx.Request.CopyTo(req)
				req.Header.Set("No-Hops","True")
				
				tmo := time.Now().Add(time.Second)
				
				// So, at this point, we are going to process, all requests in parralel.
				wg := new(comboLock)
				performer := func(peer *Peer) {
					defer wg.Done()
					resp := fasthttp.AcquireResponse()
					err := peer.Client.DoDeadline(req,resp,tmo)
					if err!=nil && resp.StatusCode()==200 {
						wg.Lock(); defer wg.Unlock()
						resp.CopyTo(&ctx.Response)
					}
					fasthttp.ReleaseResponse(resp)
				}
				
				// Start the goroutines
				s.peersLock.RLock()
				for _,peer := range s.peers {
					wg.Add(1)
					go performer(peer)
				}
				s.peersLock.RUnlock()
				
				
				// Wait for the goroutines to exit.
				wg.Wait()
				
				//Release the request-object
				fasthttp.ReleaseRequest(req)
			}
			return
		}
	case "":
		switch string(ctx.Method()) {
		case "GET":
			{
				stream := jsoniter.NewStream(jsoniter.ConfigFastest,ctx,512)
				stream.WriteObjectStart()
				stream.WriteObjectField("paritions")
				stream.WriteArrayStart()
				more := false
				for k := range s.Partitions {
					if more { stream.WriteMore() } else { more = true }
					stream.WriteString(k)
				}
				stream.WriteArrayEnd()
				stream.WriteObjectEnd()
				stream.Flush()
				return
			}
		}
	}
	if partition,ok := s.Partitions[string(part)] ; ok {
		if len(sub)==0 {
			switch string(ctx.Method()) {
			case "GET":
				{
					stream := jsoniter.NewStream(jsoniter.ConfigFastest,ctx,512)
					stream.WriteObjectStart()
					stream.WriteObjectEnd()
					stream.Flush()
					return
				}
			}
		}
		switch string(ctx.Method()) {
		case "GET":
			{
				err := partition.KVP.Get(sub,ctx)
				if err==storage.ENotFound {
					ctx.Error("Not found\n", fasthttp.StatusNotFound)
					ctx.Response.Header.Set("Error-404", "key")
				} else if err==storage.EStorageError {
					ctx.Error("Storage Error\n", fasthttp.StatusInternalServerError)
					ctx.Response.Header.Set("Error-500", "storage-corruption")
				} else if err!=nil {
					ctx.Error("Storage or IO Error\n", fasthttp.StatusInternalServerError)
					ctx.Response.Header.Set("Error-500", "IO")
				}
				return
			}
		case "PUT":
			{
				err := partition.KVP.Put(sub,ctx.Request.Body())
				if err==storage.EInsertionFailed {
					ctx.Error("Insertion Failed (Out of Storage)\n", fasthttp.StatusInsufficientStorage)
				} else if err!=nil {
					ctx.Error("Insertion Failed\n", fasthttp.StatusInternalServerError)
					ctx.Response.Header.Set("Error-500", "IO")
				}
				ctx.Error("OK\n", 200)
				return
			}
		}
	}
	if peer := s.lookupPartitionPeer(part) ; peer!=nil {
		if string(ctx.Request.Header.Peek("No-Hops"))=="True" { // Circle detected
			ctx.Error("Circular Reference\n", fasthttp.StatusVariantAlsoNegotiates)
			return
		}
		
		ctx.Request.Header.Set("No-Hops","True")
		err := peer.Client.DoDeadline(&ctx.Request, &ctx.Response, time.Now().Add(time.Second) )
		if err!=nil { ctx.Error("Bad Gateway\n", fasthttp.StatusBadGateway) }
		return
	}
	
	ctx.Error("No such partition\n", fasthttp.StatusNotFound)
	ctx.Response.Header.Set("Error-404", "partition")
}


