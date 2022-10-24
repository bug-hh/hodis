package parser

import (
	"bufio"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/hodis/redis"
	"github.com/hodis/redis/protocol"
)

type Payload struct {
	Data redis.Reply
	Err  error
}

// 这里用于解析 redis 协议 redis serialization protocol（RESP)
// 解析由客户端发来的命令
// 函数最终把解析结果放入通道返回
func ParseStream(reader io.Reader) <-chan *Payload {
	/*
		resp 协议有五种类型数据，客户端，服务端统一用 \r\n 作为换行符
		简单字符串，以 + 开头，
		错误信息，以 - 开头
		整数，以 : 开头
		字符串, 以 $ 开头
		数组, 以 * 开头
		以是否「二进制安全」可以把上述五类划分成两类，那么「简单字符串」「错误信息」「整数」非二进制安全，其余二进制安全
		二进制安全是指允许协议中出现任意字符而不会导致故障
	*/
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
	readingRepl       bool
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 参数还缺个 chan 通道
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()

	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(*bufReader, &state)
		if err != nil {
			if ioErr {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			state = readState{}
			continue
		}

		// parse line
		if !state.readingMultiLine {
			// 数组
			if msg[0] == '*' {
				// 这里主要是用来更新 state 里面的字段
				err := parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					// 要是解析有问题的话，就重置 state
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &protocol.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// 字符串
				// 更新 state 里的字段
				err := parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{}
					continue
				}

				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &protocol.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else {
				// 非二进制安全类型：「简单字符串」「错误信息」「整数」
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				log.Printf("parseSingleLineReply success: %+v", string(result.ToBytes()))
				state = readState{}
				continue
			}
		} else {
			// 按照更新后的 state 字段，来解析
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{}
				continue
			}

			if state.finished() {
				var result redis.Reply
				if state.msgType == '*' {
					result = protocol.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = protocol.MakeMultiBulkReply(state.args)
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				log.Printf("state finished: %+v", result)
				state = readState{}
			}
		}
	}
}

func parseSingleLineReply(msg []byte) (redis.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result redis.Reply
	switch msg[0] {
	case '+':
		// 简单字符串
		result = protocol.MakeStatusReply(str[1:])
	case '-':
		// 错误信息
		result = protocol.MakeErrReply(str[1:])
	case ':':
		// 整数
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = protocol.MakeIntReply(val)
	default:
		// 按照文本进行解析
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = protocol.MakeMultiBulkReply(args)
	}
	return result, nil
}

func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	// 解析这个字符串有多少个字符
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen >= 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64

	// 获取数组元素个数
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}

	if expectedLine == 0 {
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}
func readLine(bufReader bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 读取非二进制安全数据，也有可能是二进制安全数据，这是一个初始状态
	if state.bulkLen == 0 {
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 读取二进制安全数据
		// 加 2 是算上了 "\r\n"
		bulkLen := state.bulkLen + 2
		// repl 是什么？
		if state.readingRepl {
			bulkLen -= 2
		}
		msg := make([]byte, bulkLen)
		_, err = io.ReadFull(&bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		state.bulkLen = 0
	}
	return msg, false, nil
}

func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if len(line) > 0 && line[0] == '$' {
		state.bulkLen, err = strconv.ParseInt(string(msg[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen < 0 {
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}
