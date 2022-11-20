package protocol

type PongReply struct {

}

var pongBytes = []byte("+PONG\r\n")

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

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

// OkReply is +OK
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

// MakeOkReply returns a ok protocol
func MakeOkReply() *OkReply {
	return theOkReply
}
