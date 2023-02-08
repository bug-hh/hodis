package utils

import (
	"math/rand"
	"os"
	"strings"
	"time"
)

const (
	MasterRole = iota
	SlaveRole
	SentinelRole
)

func BytesEquals(a, b []byte) bool {
	if (a == nil && b != nil) || (a != nil && b == nil) {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	size := len(a)
	for i := 0; i < size; i++ {
		av := a[i]
		bv := b[i]
		if av != bv {
			return false
		}
	}
	return true
}

// ToCmdLine convert strings to [][]byte
func ToCmdLine(cmd ...string) [][]byte {
	args := make([][]byte, len(cmd))
	for i, s := range cmd {
		args[i] = []byte(s)
	}
	return args
}

// ToCmdLine2 convert commandName and string-type argument to [][]byte
func ToCmdLine2(commandName string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

// ToCmdLine3 convert commandName and []byte-type argument to CmdLine
func ToCmdLine3(commandName string, args ...[]byte) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(commandName)
	for i, s := range args {
		result[i+1] = []byte(s)
	}
	return result
}

func FormatCmdLine(cmdLine [][]byte) string {
	sArr := make([]string, 0, len(cmdLine))
	for _, cmd := range cmdLine {
		sArr = append(sArr, string(cmd))
	}
	return strings.Join(sArr, " ")
}

// 获取 [0, n) 范围内的随机数
func RandomInt(n int) int {
	//使用当前时间戳 生成真正的随机数字
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//取二以下的正数 0,1
	return r.Intn(n)
}

//PathExists 判断一个文件或文件夹是否存在
//输入文件路径，根据返回的bool值来判断文件或文件夹是否存在
func PathExists(path string) bool {
	info, err := os.Stat(path)
	if err == nil {
		return info.Size() > 0
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}