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


package msgpackiter

import "encoding/binary"
import "math"

var bE = binary.BigEndian

type ValueType int
const (
	InvalidType ValueType = iota
	IntType
	FloatType
	StringType // Byte string (Text)
	BinaryType // Byte string (Binary)
	ArrayType
	MapType
	NilType
	BoolType
)

func PeekValue(buf []byte) (ret ValueType) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0x80)==0 { return IntType } // Fixint
	if (fb&0xe0)==0xe0 { return IntType }
	if (fb&0xe0)==0xa0 { return StringType }
	if (fb&0xf0)==0x90 { return ArrayType }
	if (fb&0xf0)==0x80 { return MapType }
	switch fb {
	case 0xc0: return NilType
	case 0xc2,0xc3: return BoolType
	case 0xcc,0xcd,0xce,0xcf,0xd0,0xd1,0xd2,0xd3: return IntType
	case 0xca,0xcb: return FloatType
	case 0xd9,0xda,0xdb: return StringType
	case 0xc4,0xc5,0xc6: return BinaryType
	case 0xdc,0xdd: return ArrayType
	case 0xde,0xdf: return ArrayType
	}
	return
}

func ReadBool(buf []byte) (b bool,l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	switch fb {
	case 0xc2: return false,1
	case 0xc3: return true,1
	}
	return
}
func ReadArrayLength(buf []byte) (o,l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0xf0)==0x90 { return 1,int(fb&0xf) }
	if (fb&0xf0)==0x80 { return 1,int(fb&0xf)<<1 }
	switch fb {
	case 0xdc: if len(buf)<3 { return } ; return 3,int(bE.Uint16(buf[1:]))
	case 0xde: if len(buf)<3 { return } ; return 3,int(bE.Uint16(buf[1:]))<<1
	case 0xdd: if len(buf)<5 { return } ; return 5,int(bE.Uint32(buf[1:]))
	case 0xdf: if len(buf)<5 { return } ; return 5,int(bE.Uint32(buf[1:]))<<1
	}
	return
}

func StringRangeLength(buf []byte) (b,e int) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0xe0)==0xa0 { return 1,int(fb&0x1f)+1 }
	switch fb {
	case 0xd9,0xc4: if len(buf)<2 { return } ; return 2,int(buf[1])+2
	case 0xda,0xc5: if len(buf)<3 { return } ; return 3,int(bE.Uint16(buf[1:]))+3
	case 0xdb,0xc6: if len(buf)<5 { return } ; return 5,int(bE.Uint32(buf[1:]))+5
	}
	return
}
func ScalarLength(buf []byte) (l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0x80)==0 { return 1 } // Fixint
	if (fb&0xe0)==0xe0 { return 1 }
	if (fb&0xe0)==0xa0 { return int(fb&0x1f)+1 }
	switch fb {
	case 0xc0,0xc2,0xc3: return 1
	case 0xcc,0xd0: return 2
	case 0xcd,0xd1: return 3
	case 0xce,0xd2,0xca: return 5
	case 0xcf,0xd3,0xcb: return 9
	case 0xd9,0xc4: if len(buf)<2 { return }; return int(buf[1])+2
	case 0xda,0xc5: if len(buf)<3 { return }; return int(bE.Uint16(buf[1:]))+3
	case 0xdb,0xc6: if len(buf)<5 { return }; return int(bE.Uint32(buf[1:]))+5
	}
	return
}

func ReadInt(buf []byte) (v int64,l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0x80)==0 { return int64(fb&0x7f),1 } // Fixint
	//if (fb&0xe0)==0xe0 { return -int64(fb&0x1f),1 }
	if (fb&0xe0)==0xe0 { return int64(int8(fb)),1 }
	switch fb {
	case 0xcc: return int64(buf[1]),2
	case 0xcd: return int64(bE.Uint16(buf[1:])),3
	case 0xce: return int64(bE.Uint32(buf[1:])),5
	case 0xcf: return int64(bE.Uint64(buf[1:])),9 // Warning: Overflow
	case 0xd0: return int64(int8(buf[1])),2
	case 0xd1: return int64(int16(bE.Uint16(buf[1:]))),3
	case 0xd2: return int64(int32(bE.Uint32(buf[1:]))),5
	case 0xd3: return int64(bE.Uint64(buf[1:])),9
	}
	return
}

func ReadUint(buf []byte) (v uint64,l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	if (fb&0x80)==0 { return uint64(fb&0x7f),1 } // Fixint
	if (fb&0xe0)==0xe0 { return uint64(-int64(fb&0x1f)),1 }
	switch fb {
	case 0xcc: return uint64(buf[1]),2
	case 0xcd: return uint64(bE.Uint16(buf[1:])),3
	case 0xce: return uint64(bE.Uint32(buf[1:])),5
	case 0xcf: return uint64(bE.Uint64(buf[1:])),9 // Warning: Overflow
	case 0xd0: return uint64(int8(buf[1])),2
	case 0xd1: return uint64(int16(bE.Uint16(buf[1:]))),3
	case 0xd2: return uint64(int32(bE.Uint32(buf[1:]))),5
	case 0xd3: return uint64(bE.Uint64(buf[1:])),9
	}
	return
}

func ReadFloat(buf []byte) (f float64,l int) {
	if len(buf)==0 { return }
	fb := buf[0]
	switch fb {
	case 0xca: return float64(math.Float32frombits(bE.Uint32(buf[1:]))),5
	case 0xcb: return math.Float64frombits(bE.Uint64(buf[1:])),9
	}
	return
}


