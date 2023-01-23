package redis

// redis client 连接
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// client should keep its subscribing channels
	Subscribe(channel string)
	UnSubscribe(channel string)

	PSubscribe(channel string)
	PUnSubscribe(channel string)

	SubsCount() int
	PSubsCount() int
	GetChannels() []string

	// used for `Multi` command
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueuedCmds()
	GetWatching() map[string]uint32
	AddTxError(err error)
	GetTxErrors() []error

	// used for multi database
	GetDBIndex() int
	SelectDB(int)
	// returns role of conn, such as connection with client, connection with master node
	GetRole() int32
	SetRole(int32)
}