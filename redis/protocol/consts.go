package protocol

var emptyMultiBulkBytes = []byte("*0\r\n")

type EmptyMultiBulkReply struct {
}

func (e *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

var nullBulkBytes = []byte("$-1\r\n")

type NullBulkReply struct {
}

func (n *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}
