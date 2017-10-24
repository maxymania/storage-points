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


package levelfile

import "io"
import "io/ioutil"
import "os"
import "path/filepath"
import "encoding/binary"

import "github.com/syndtr/goleveldb/leveldb"
import "github.com/maxymania/storage-points/storage"
import "github.com/maxymania/storage-points/storage/loader"

import "github.com/maxymania/storage-points/storage/filestore"

import "github.com/vmihailenco/msgpack"
import mpacki "github.com/maxymania/storage-points/msgpackiter"

import "github.com/byte-mug/golibs/buffer"
import "sync"
import "fmt"

type FilePartition struct{
	DB *leveldb.DB
	SM filestore.StorageManager
	
	MinSize      int
	MaxFileSpace int64
	
	Path         string
	
	// Synchronized group
	locker sync.Mutex
	freeMap  map[int64]int64
	lastFree int64
}

func (s *FilePartition) findFree(n int) (int64,int64,*filestore.FileEntry) {
	s.locker.Lock(); defer s.locker.Unlock()
	// look in the free-map
	for k,siz := range s.freeMap {
		if siz<int64(n) { continue }
		fobj,err := s.SM.Open(k)
		if err!=nil { continue }
		spf := fobj.ApproxFreeSpaceFor(n)
		if spf < int64(n) { fobj.Decr(); continue } // estimination failed or negative
		spc,err := fobj.Alloc(n)
		if err!=nil || spc<512 { fobj.Decr(); continue } // Allocation failed
		
		// Update free-map.
		s.freeMap[k] = fobj.ApproxFreeSpace()
		
		return k,spc,fobj // Yay, we found it!
	}
	// look forward after last-free
	for {
		k := s.lastFree
		s.lastFree = k+1
		fobj,err := s.SM.Open(k)
		if err!=nil { continue }
		siz := fobj.ApproxFreeSpace()
		s.freeMap[k] = siz
		if siz<int64(n) { fobj.Decr(); continue }
		
		spf := fobj.ApproxFreeSpaceFor(n)
		if spf < int64(n) { fobj.Decr(); continue } // estimination failed or negative
		spc,err := fobj.Alloc(n)
		if err!=nil || spc<512 { fobj.Decr(); continue } // Allocation failed
		
		// Update free-map.
		s.freeMap[k] = fobj.ApproxFreeSpace()
		
		return k,spc,fobj // Yay, we found it!
	}
	return 0,0,nil
}
func (s *FilePartition) free(num,off int64,fobj *filestore.FileEntry) {
	fobj.Free(off)
	s.locker.Lock(); defer s.locker.Unlock()
	s.freeMap[num] = fobj.ApproxFreeSpace()
}
func (s *FilePartition) free2(num,off int64) {
	fobj,err := s.SM.Open(num)
	if err!=nil { return }
	defer fobj.Decr()
	s.free(num,off,fobj)
}
func (s *FilePartition) insert(value []byte) (/*filenum*/int64,/*offset*/int64,error) {
	var bitbuf [4]byte
	binary.BigEndian.PutUint32(bitbuf[:],uint32(len(value)))
	
	num,off,file := s.findFree(len(value)+4)
	if file==nil { return 0,0,storage.EInsertionFailed }
	defer file.Decr()
	_,err := file.WriteAt(bitbuf[:],off)
	if err!=nil {
		s.free(num,off,file)
		return 0,0,err
	}
	_,err = file.WriteAt(value,off+4)
	if err!=nil {
		s.free(num,off,file)
		return 0,0,err
	}
	
	return num,off,nil
}
func (s *FilePartition) Put(id, value []byte) error {
	dbuf,err := s.DB.Get(id,nil)
	if err==leveldb.ErrNotFound { err = nil; dbuf = nil }
	if err!=nil { return err } // IO-Error
	
	vt := mpacki.PeekValue(dbuf)
	
	if vt==mpacki.IntType {
		l := mpacki.ScalarLength(dbuf)
		if len(dbuf)<l { goto performInsert }
		filenum,_ := mpacki.ReadInt(dbuf)
		dbuf = dbuf[l:]
		
		l = mpacki.ScalarLength(dbuf)
		if len(dbuf)<l { goto performInsert }
		offset,_ := mpacki.ReadInt(dbuf)
		
		fobj,err := s.SM.Open(filenum)
		if err!=nil { return err }
		defer fobj.Decr()
		
		if len(value)==0 {
			err = s.DB.Delete(id,nil)
			if err!=nil { return err }
			fobj.Free(offset)
			
			s.locker.Lock()
			s.freeMap[filenum] = fobj.ApproxFreeSpace()
			s.locker.Unlock()
			
			return nil
		}
		if len(value) > ((24<<20)-20) { return storage.EStorageError }
		
		sz,err := fobj.UsableSize(offset)
		if err!=nil { return err }
		
		// In-Place Update
		if sz>=(len(value)+4) {
			var bitbuf [4]byte
			binary.BigEndian.PutUint32(bitbuf[:],uint32(len(value)))
			_,err = fobj.WriteAt(bitbuf[:],offset)
			if err!=nil { return err }
			_,err = fobj.WriteAt(value,offset+4)
			return err
		}
		
		if len(value)<s.MinSize {
			stuff,_ := msgpack.Marshal(value)
			err = s.DB.Put(id,stuff,nil)
			if err!=nil { return err }
		} else {
			nnum,noff,err := s.insert(value)
			if err!=nil { return err }
			stuff,_ := msgpack.Marshal(nnum,noff)
			err = s.DB.Put(id,stuff,nil)
			if err!=nil { s.free2(nnum,noff) ; return err }
		}
		
		s.free(filenum,offset,fobj)
		
		return nil
	}
	performInsert:
	
	if len(value)==0 {
		if len(dbuf)>0 { return s.DB.Delete(id,nil) }
		return nil
	}
	
	if len(value)<s.MinSize {
		stuff,_ := msgpack.Marshal(value)
		err = s.DB.Put(id,stuff,nil)
		if err!=nil { return err }
	} else {
		nnum,noff,err := s.insert(value)
		if err!=nil { return err }
		stuff,_ := msgpack.Marshal(nnum,noff)
		err = s.DB.Put(id,stuff,nil)
		if err!=nil { s.free2(nnum,noff) ; return err }
	}
	
	return nil
}
func (s *FilePartition) Get(id []byte, dest io.Writer) error {
	dbuf,err := s.DB.Get(id,nil)
	if err!=nil {
		if err==leveldb.ErrNotFound { err = storage.ENotFound }
		return err
	}
	
	switch mpacki.PeekValue(dbuf) {
	case mpacki.StringType,mpacki.BinaryType:
		{
			b,e := mpacki.StringRangeLength(dbuf)
			if e==0 || len(dbuf)<e { return storage.EStorageError }
			_,err = dest.Write(dbuf[b:e])
			return err
		}
	case mpacki.IntType:
		{
			l := mpacki.ScalarLength(dbuf)
			if len(dbuf)<l { return storage.EStorageError }
			filenum,_ := mpacki.ReadInt(dbuf)
			dbuf = dbuf[l:]
			
			l = mpacki.ScalarLength(dbuf)
			if len(dbuf)<l { return storage.EStorageError }
			offset,_ := mpacki.ReadInt(dbuf)
			
			fobj,err := s.SM.Open(filenum)
			if err!=nil { return err }
			defer fobj.Decr()
			var bitbuf [4]byte
			
			_,err = fobj.ReadAt(bitbuf[:],offset)
			if err!=nil { return err }
			
			size := binary.BigEndian.Uint32(bitbuf[:])
			if size > ((24<<20)-20) { return storage.EStorageError }
			b := buffer.Get(int(size))
			defer buffer.Put(b)
			_,err = fobj.ReadAt((*b)[:size],offset+4)
			if err!=nil { return err }
			_,err = dest.Write((*b)[:size])
			return err
		}
	}
	
	_,err = dest.Write(dbuf)
	return err
}
func (s *FilePartition) GetFreeSpace() int64 {
	if s.MaxFileSpace==0 { return 0 }
	space := s.MaxFileSpace
	infos,err := ioutil.ReadDir(s.Path)
	if err!=nil { return 0 }
	s.locker.Lock()
		for _,sp := range s.freeMap {
			space -= s.SM.MaxFileSize-sp
		}
		lastFree := s.lastFree
	s.locker.Unlock()
	var num int64
	for _,info := range infos {
		if _,err := fmt.Sscanf(info.Name(),"%06d.dat",&num); err!=nil { continue }
		if num<lastFree { continue }
		space -= info.Size()
	}
	if space<0 { return 0 }
	return space
}

type Config struct{
	MinSize      int
	MaxOpenFiles int
	MaxFileSize  int64
	MaxFileSpace int64
}
func (s *Config) OpenKVP(path string) (storage.KeyValuePartition,error) {
	ldb := filepath.Join(path,"levelidx")
	os.Mkdir(ldb, 0700)
	db,err := leveldb.OpenFile(ldb,nil)
	if err!=nil { return nil,err }
	fp := new(FilePartition)
	fp.DB = db
	fp.SM.Init(filestore.Dir(path))
	fp.SM.MaxOpenFiles = s.MaxOpenFiles
	fp.SM.MaxFileSize  = s.MaxFileSize
	fp.MaxFileSpace    = s.MaxFileSpace
	fp.MinSize         = s.MinSize
	fp.freeMap = make(map[int64]int64)
	return fp,nil
}

func init(){
	loader.Backends["levelfile"] = &Config{
		MinSize: 64,
		MaxOpenFiles: 100,
	}
}

