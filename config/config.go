package config

import (
	"bufio"
	"github.com/hodis/lib/logger"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
)

const (
	CLUSTER_MODE = iota
	STANDALONE
	SENTINEL
)

type ServerProperties struct {
	Bind string `cfg:"bind"`
	Port int `cfg:"port"`
	AppendOnly bool `cfg:"appendonly"`
	AppendFilename    string `cfg:"appendfilename"`
	MaxClients int `cfg:"maxclients"`
	RequirePass       string `cfg:"requirepass"`
	Databases         int    `cfg:"databases"`
	RDBFilename       string `cfg:"dbfilename"`
	MasterAuth        string `cfg:"masterauth"`
	SlaveAnnouncePort int    `cfg:"slave-announce-port"`
	SlaveAnnounceIP   string `cfg:"slave-announce-ip"`
	ReplTimeout       int    `cfg:"repl-timeout"`

	Peers []string `cfg:"peers"`
	Self  string   `cfg:"self"`

	//任何执行时长大于或等于它的命令，都会被认为是慢查询命令，都会被慢查询日志记录下来, 单位是微秒
	SlowLogLogSlowerThan int64 `cfg:"slowlog-log-slower-than"`
	SlowLogMaxLen int `cfg:"slowlog-max-len"`

	//Sentinel *sentinel.SentinelState
	Sentinel map[string]map[string]interface{}

	ServerMode int
}

// Properties holds global config properties
var Properties *ServerProperties

func init() {
	// default config
	Properties = &ServerProperties{
		Bind:       "127.0.0.1",
		Port:       6379,
		AppendOnly: false,
		SlowLogLogSlowerThan: -1,
		Sentinel: make(map[string]map[string]interface{}),
		// 默认单机模式
		ServerMode: STANDALONE,
	}
}

func generateRawMap(src io.Reader) map[string]string {
	rawMap := make(map[string]string)
	scanner := bufio.NewScanner(src)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 && line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot > 0 && pivot < len(line)-1 { // separator found
			key := line[0:pivot]
			value := strings.Trim(line[pivot+1:], " ")
			rawMap[strings.ToLower(key)] = value
		}
	}
	if err := scanner.Err(); err != nil {
		logger.Fatal(err)
	}
	return rawMap
}

func parse(src io.Reader) *ServerProperties {
	config := &ServerProperties{}

	// read config file
	rawMap := generateRawMap(src)
	//rawMap := make(map[string]string)
	//scanner := bufio.NewScanner(src)
	//for scanner.Scan() {
	//	line := scanner.Text()
	//	if len(line) > 0 && line[0] == '#' {
	//		continue
	//	}
	//	pivot := strings.IndexAny(line, " ")
	//	if pivot > 0 && pivot < len(line)-1 { // separator found
	//		key := line[0:pivot]
	//		value := strings.Trim(line[pivot+1:], " ")
	//		rawMap[strings.ToLower(key)] = value
	//	}
	//}
	//if err := scanner.Err(); err != nil {
	//	logger.Fatal(err)
	//}

	// parse format
	t := reflect.TypeOf(config)
	v := reflect.ValueOf(config)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		value, ok := rawMap[strings.ToLower(key)]
		if ok {
			// fill config
			switch field.Type.Kind() {
			case reflect.String:
				fieldVal.SetString(value)
			case reflect.Int:
				intValue, err := strconv.ParseInt(value, 10, 64)
				if err == nil {
					fieldVal.SetInt(intValue)
				}
			case reflect.Bool:
				boolValue := "yes" == value
				fieldVal.SetBool(boolValue)
			case reflect.Slice:
				if field.Type.Elem().Kind() == reflect.String {
					slice := strings.Split(value, ",")
					fieldVal.Set(reflect.ValueOf(slice))
				}
			}
		} else {
			slowKey := strings.ToLower(key)
			switch field.Type.Kind() {
			case reflect.Int64:
				// 如果用户没有指定慢查询日志选项，那么设置为 -1，表示不进行慢查询日志记录
				if slowKey == "slowlog-log-slower-than" {
					fieldVal.SetInt(-1)
				}
			case reflect.Int:
				// 如果用户没有指定慢查询日志选项，那么设置为 -1，表示不进行慢查询日志记录
				if slowKey == "slowlog-max-len" {
					fieldVal.SetInt(-1)
				}
			}
		}
	}
	return config
}

func parseSentinelConfigFile(src io.Reader) *ServerProperties {
	config := &ServerProperties{
		Bind: "127.0.0.1",
		Sentinel: make(map[string]map[string]interface{}),
	}

	scanner := bufio.NewScanner(src)
	masterName := ""
	masterIp := ""
	masterPort := 0
	quorum := 0
	downAfterMilliseconds := 0
	parallelSyncs := 0

	for scanner.Scan() {
		line := scanner.Text()
		if (len(line) > 0 && line[0] == '#') || len(line) < 3 {
			continue
		}
		// 获取被空格分割的元素列表
		items := strings.Fields(line)
		if items[0] == "port" {
			config.Port, _ = strconv.Atoi(items[1])
		} else if items[0] == "sentinel" {
			if items[1] == "monitor" && len(items) == 6 {
				masterName = items[2]
				if _, exists := config.Sentinel[masterName]; !exists {
					config.Sentinel[masterName] = make(map[string]interface{})
				}
				masterIp = items[3]
				masterPort, _ = strconv.Atoi(items[4])
				quorum, _ = strconv.Atoi(items[5])

				config.Sentinel[masterName]["quorum"] = quorum
				config.Sentinel[masterName]["master_name"] = masterName
				config.Sentinel[masterName]["ip"] = masterIp
				config.Sentinel[masterName]["port"] = masterPort
			} else if items[1] == "down-after-milliseconds" {
				downAfterMilliseconds, _ = strconv.Atoi(items[3])
				config.Sentinel[masterName]["down_after_milliseconds"] = downAfterMilliseconds
			} else if items[1] == "parallel-syncs" {
				parallelSyncs, _ = strconv.Atoi(items[3])
				config.Sentinel[masterName]["parallel_syncs"] = parallelSyncs
			} else {
				logger.Warn("unknown sentinel config")
			}
		}
	}

	return config
}

// SetupConfig read config file and store properties into Properties
func SetupConfig(configFilename string) {
	file, err := os.Open(configFilename)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parse(file)
}

func SetupSentinelConfig(configFileName string) {
	file, err := os.Open(configFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	Properties = parseSentinelConfigFile(file)
}

const (
	AllDefaultConfigFileName = "default_config.conf"
)

// 读取已载入内存配置 Properties
func getOnlineConfig() map[string]string {
	onlineMap := make(map[string]string)
	t := reflect.TypeOf(Properties)
	v := reflect.ValueOf(Properties)
	n := t.Elem().NumField()
	for i := 0; i < n; i++ {
		field := t.Elem().Field(i)
		fieldVal := v.Elem().Field(i)
		key, ok := field.Tag.Lookup("cfg")
		if !ok {
			key = field.Name
		}
		switch field.Type.Kind() {
		case reflect.String:
			onlineMap[key] = fieldVal.String()
		case reflect.Int:
			onlineMap[key] = strconv.FormatInt(fieldVal.Int(), 10)
		case reflect.Bool:
			if fieldVal.Bool() {
				onlineMap[key] = "yes"
			} else {
				onlineMap[key] = "no"
			}
		case reflect.Slice:
			var temp []string
			for j:=0;j<fieldVal.Len();j++ {
				temp = append(temp, fieldVal.Index(i).String())
			}
			onlineMap[key] = strings.Join(temp, ",")
		}
	}
	return onlineMap
}

func ReadAllConfig() map[string]string {
	file, err := os.Open(AllDefaultConfigFileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 先读默认配置, 并保存到一个 map 中
	rawMap := generateRawMap(file)
	// 再读出已经载入的配置
	onlineMap := getOnlineConfig()

	// 用线上配置覆盖默认配置
	for k, v := range onlineMap {
		rawMap[k] = v
	}

	return rawMap
}