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


// OkReply is +OK
type OOKKReply struct{}

var OOKKBytes = []byte("+OOKK\r\n")

// ToBytes marshal redis.Reply
func (r *OOKKReply) ToBytes() []byte {
	return OOKKBytes
}

var theOOKKReply = new(OOKKReply)

// MakeOkReply returns a ok protocol
func MakeOOKKReply() *OOKKReply {
	return theOOKKReply
}

// 事务相关，命令入队 reply
// QueuedReply is +QUEUED
type QueuedReply struct{}

var queuedBytes = []byte("+QUEUED\r\n")

// ToBytes marshal redis.Reply
func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

var theQueuedReply = new(QueuedReply)

// MakeQueuedReply returns a QUEUED protocol
func MakeQueuedReply() *QueuedReply {
	return theQueuedReply
}

// NoReply respond nothing, for commands like subscribe
type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}