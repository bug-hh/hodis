package connection

import (
	"github.com/hodis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

const (
	// NormalCli is client with user
	NormalCli = iota
	// ReplicationRecvCli is fake client with replication master
	ReplicationRecvCli
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	waitingReply wait.Wait

	mu sync.Mutex

	subs map[string]bool

	password string

	multiState bool
	queue [][][]byte
	watching map[string]uint32
	txErrors []error

	selectedDB int
	role int32
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

/*
实现 redis.Connection 这个接口的所有方法
 */
func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
	}()
	_, err := c.conn.Write(b)
	return err
}

/*
暂时保持空实现
 */
func (c *Connection) Subscribe(channel string) {

}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {

}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	return len(c.subs)
}


// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

/*
multi 命令是用于开启事务的
 */
// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.multiState
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
	}
	c.multiState = state
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues command of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}


// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

// GetRole returns role of connection, such as connection with master
func (c *Connection) GetRole() int32 {
	if c == nil {
		return NormalCli
	}
	return c.role
}

func (c *Connection) SetRole(r int32) {
	c.role = r
}

// GetDBIndex returns selected db
func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

// SelectDB selects a database
func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}
