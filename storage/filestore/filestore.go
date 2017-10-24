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


package filestore

import "container/list"
import "github.com/byte-mug/golibs/filealloc"
import "sync"
import "sync/atomic"

type Storage interface{
	Open(num int64) (filealloc.File,error)
}

type FileEntry struct{
	refc  int64
	lock  sync.Mutex
	elem  *list.Element
	man   *StorageManager
	num   int64
	file  filealloc.File
	alloc *filealloc.Allocator
}
func (f *FileEntry) Incr() {
	atomic.AddInt64(&f.refc,1)
}
func (f *FileEntry) Decr() {
	if atomic.AddInt64(&f.refc,-1)<=0 {
		atomic.StoreInt64(&f.refc,0) // Just in case we went below.
		f.file.Close()
		f.file = nil
		f.alloc = nil
		f.man.pool.Put(f) // Release the object, so it can be reused.
	}
}
func (f *FileEntry) Alloc(size int) (int64, error) {
	fsz := f.alloc.FileSize()
	noGrow := ( f.man.GetMaxFileSize() - fsz ) < int64(size)
	return f.alloc.Alloc(size,noGrow)
}
func (f *FileEntry) Free(off int64) error {
	return f.alloc.Free(off)
}
func (f *FileEntry) UsableSize(off int64) (int, error) {
	return f.alloc.UsableSize(off)
}
func (f *FileEntry) ApproxFreeSpace() (total int64) {
	total = f.man.GetMaxFileSize() - f.alloc.FileSize()
	if total<0 { total = 0 }
	total += f.alloc.ApproxFreeSpace()
	return
}

type StorageManager struct{
	lock sync.Mutex
	list *list.List
	mp   map[int64]*FileEntry
	sb   Storage
	pool sync.Pool
	MaxOpenFiles int
	MaxFileSize int64 // Default is one TB
}
func (s *StorageManager) GetMaxFileSize() int64 {
	mfs := s.MaxFileSize
	if mfs == 0 { mfs = 1<<40 }
	return mfs
}
func (s *StorageManager) Init(sb Storage) *StorageManager {
	s.mp = make(map[int64]*FileEntry)
	s.sb = sb
	s.list = list.New()
	return s
}
func (s *StorageManager) pullOld() {
	mof := s.MaxOpenFiles
	if mof<4 { mof = 4 }
	for s.list.Len()>mof {
		
		// Get the last element
		ele := s.list.Back()
		fe := ele.Value.(*FileEntry)
		
		// Paranoid
		if ele != fe.elem {
			s.list.Remove(ele)
			continue
		}
		
		// Remove it from the list
		s.list.Remove(ele)
		fe.elem=nil
		
		// Remove it from the map
		delete(s.mp,fe.num)
		
		// Decrement
		fe.Decr()
	}
}
func (s *StorageManager) Open(num int64) (*FileEntry,error) {
	s.lock.Lock(); defer s.lock.Unlock()
	
	// Get the file from the Cache, if possible.
	fe,ok := s.mp[num]
	fe.Incr()
	if ok {
		if fe.elem!=nil { // Safety first
			s.list.MoveToFront(fe.elem)
		}
		return fe,nil
	}
	
	// Open and Initialize the file
	llf,err := s.sb.Open(num)
	if err!=nil { return nil,err }
	fal,err := filealloc.NewAllocator(llf)
	if err!=nil { llf.Close(); return nil,err }
	
	// Allocate a FileEntry  --- (from the pool or new)
	fe,ok = s.pool.Get().(*FileEntry)
	if !ok { fe = &FileEntry{man:s} }
	
	// Set all fields
	fe.file = llf
	fe.alloc = fal
	fe.num = num
	
	// Put it to the cache
	fe.Incr()
	s.mp[fe.num] = fe
	fe.elem = s.list.PushFront(fe)
	
	// Return it
	fe.Incr()
	return fe,nil
}


