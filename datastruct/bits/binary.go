package bits

var BitsInBytes = [256]byte{
	0,1,1,2,1,2,2,3,1,2,2,3,
	2,3,3,4,1,2,2,3,2,3,3,4,
	2,3,3,4,3,4,4,5,1,2,2,3,
	2,3,3,4,2,3,3,4,3,4,4,5,
	2,3,3,4,3,4,4,5,3,4,4,5,
	4,5,5,6,1,2,2,3,2,3,3,4,
	2,3,3,4,3,4,4,5,2,3,3,4,
	3,4,4,5,3,4,4,5,4,5,5,6,
	2,3,3,4,3,4,4,5,3,4,4,5,
	4,5,5,6,3,4,4,5,4,5,5,6,
	4,5,5,6,5,6,6,7,1,2,2,3,
	2,3,3,4,2,3,3,4,3,4,4,5,
	2,3,3,4,3,4,4,5,3,4,4,5,
	4,5,5,6,2,3,3,4,3,4,4,5,
	3,4,4,5,4,5,5,6,3,4,4,5,
	4,5,5,6,4,5,5,6,5,6,6,7,
	2,3,3,4,3,4,4,5,3,4,4,5,
	4,5,5,6,3,4,4,5,4,5,5,6,
	4,5,5,6,5,6,6,7,3,4,4,5,
	4,5,5,6,4,5,5,6,5,6,6,7,
	4,5,5,6,5,6,6,7,5,6,6,7,
	6,7,7,8,
}

type BinaryBit []byte

func Make() *BinaryBit {
	ret := BinaryBit(make([]byte, 0))
	return &ret
}

func MakeFromBytes(bs []byte) *BinaryBit {
	ret := BinaryBit(bs)
	return &ret
}

func getByteSize(size int64) int64 {
	if size % 8 == 0 {
		return size/8
	}
	return size/8 + 1
}

func (b *BinaryBit) grow(bitSize int64) {
	byteSize := getByteSize(bitSize)
	diff := byteSize - int64(len(*b))
	if diff < 0 {
		return
	}
	*b = append(*b, make([]byte, diff)...)
}

func (b *BinaryBit) SetBits(offset int64, value int) {
	bitsIndex := offset / 8
	bitsOffset := offset % 8
	mask := byte(1 << bitsOffset)
	b.grow(offset+1)
	if value > 0 {
		(*b)[bitsIndex] |= mask
	} else {
		// clear bit
		/*
			操作符 &^，按位置零，例如：z = x &^ y，
			表示如果 y 中的 bit 位为 1，则 z 对应 bit 位为 0，
			否则 z 对应 bit 位等于 x 中相应的 bit 位的值。
		*/
		(*b)[bitsIndex] &^= mask
	}
}

func (b *BinaryBit) GetBits(offset int64) byte {
	bitsIndex := offset / 8
	bitsOffset := offset % 8

	if bitsIndex > int64(len(*b)) {
		return 0
	}
	return ((*b)[bitsIndex] >> bitsOffset) & 1
}

/*
求 n 的二进制表示中，1 的个数
n 是一个 32bit（占用4个字节）的整数
 */
func Swar(n uint32) uint32 {
	n = n & 0x55555555 + ((n >> 1) & 0x55555555)
	n = n & 0x33333333 + ((n >> 2) & 0x33333333)
	n = n & 0x0f0f0f0f + ((n >> 4) & 0x0f0f0f0f)
	n = (n * 0x01010101) >> 24

	return n
}

func (b *BinaryBit) BitsCount(start int, end int) uint32 {
	totalBytes := len(*b)
	totalBits := totalBytes * 8

	if (end < 0 && totalBytes + end < 0) || end > totalBytes || (start < 0 && totalBytes + start < 0) || start > totalBytes {
		return 0
	}

	if start < 0 {
		start = totalBytes + start
	}

	if end < 0 {
		end = totalBytes + end
	}

	if start > end {
		return 0
	}

	var n uint32
	var res uint32
	bs := (*b)[start:end+1]
	i := 0
	totalBytes = end - start + 1
	totalBits = totalBytes * 8
	for totalBits >= 128 {
		// 把这 4 个字节拼成一个 32 位的数
		n |= uint32(bs[i+0]) << 24
		n |= uint32(bs[i+1]) << 16
		n |= uint32(bs[i+2]) << 8
		n |= uint32(bs[i+3])
		i+=4
		res += Swar(n)
		n = 0
		totalBits -= 32
	}

	for totalBits > 0 {
		res += uint32(BitsInBytes[bs[i]])
		i++
		totalBits -= 8
	}
	return res
}