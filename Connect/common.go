package Connect

import (
	"net"
	"strconv"
	"strings"
	"unsafe"
)

// -------------------------------------------

// string转ytes
func Str2sbyte(s string) (b []byte) {
	*(*string)(unsafe.Pointer(&b)) = s                                                  // 把s的地址付给b
	*(*int)(unsafe.Pointer(uintptr(unsafe.Pointer(&b)) + 2*unsafe.Sizeof(&b))) = len(s) // 修改容量为长度
	return
}

// []byte转string
func Sbyte2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// -------------------------------------------

type ErrorInConnectAll int8 // 特用于connectAll的返回值 因为有三种错误 无法用bool表示

const (
	time_out = iota
	http_error
)

func (err ErrorInConnectAll) Error() string {
	var ans string
	switch err {
	case time_out:
		ans = "Connection to a target server greater than or equal to timeout."
		break
	case http_error:
		ans = "rpc.DialHTTP return HTTP error."
		break
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInStartServer int8 // 特用于StartServer与StartClient的返回值 因为有四种错误 无法用bool表示

const (
	parser_error = iota
	connect_error
	Listener_error
)

func (err ErrorInStartServer) Error() string {
	var ans string
	switch err {
	case parser_error:
		ans = "Error parsing configuration file."
		break
	case connect_error:
		ans = "Connection error, Refer to ErrorInConnectAll for specific error types."
		break
	case Listener_error:
		ans = "Listener error, Guess that the init function of read_c/s_config.go is not started at the call site."
		break
	default:

	}
	return ans
}

// -------------------------------------------

type ErrorInParserConfig int8

const (
	maxreries_to_small = iota
	serveraddress_length_to_small
	serveraddress_format_error
	parser_port_error
	time_out_entry_error
)

func (err ErrorInParserConfig) Error() string {
	var ans string
	switch err {
	case maxreries_to_small:
		ans = "Maxreries Less than or equal to 7."
		break
	case serveraddress_length_to_small:
		ans = "Serveraddress length Less than or equal to 2."
		break
	case serveraddress_format_error:
		ans = "Format error in ip address parsing."
		break
	case parser_port_error:
		ans = "Parser port error."
		break
	case time_out_entry_error:
		ans = "Time out entry Too small or too big."
		break
	default:
	}
	return ans
}

/*
 * @brief: 解析MyPort
 * @return: 正确返回true；否则false
 */
func parserMyPort(MyPort string) bool {
	if len(MyPort) < 0 { // 必须解析出来
		return false
	}

	var data []byte = Str2sbyte(MyPort)

	if data[0] != ':' { // 第一位必须是':'
		return false
	}

	var str string = string(data[1:])
	// TODO tcp端口的数据类型是unsigned short，因此本地端口个数最大只有65536，可能未来需要修改
	if port, err := strconv.Atoi(str); err != nil || port > 65536 || port < 0 {
		return false
	}
	return true
}

/*
 * @brief: 解析从json提取出的ip地址项
 * @return: 正确返回true；否则false
 * @notes: net包的ip解析函数有点蛋疼,没办法解析"ip:port",只能解析"ip/port"，或者"ip"
 */
func ParserIP(address string) bool {
	data := Str2sbyte(address)
	var index int = -1
	for i := len(data) - 1; i >= 0; i-- {	// 解析出最后一个‘:’
		if data[i] == ':' {
			index = i
			break
		}
	}
	if index == -1 {	// 未解析出`:`
		return false
	}

	ip := data[:index]
	port := data[index + 1:]

	ParserRes := net.ParseIP(Sbyte2str(ip))	// 解析ip "localhost"无法被ParseIP解析
	if ParserRes == nil && strings.ToLower(Sbyte2str(ip)) != "localhost"{	// 忽略大小写
		return false
	}

	if po, err := strconv.Atoi(Sbyte2str(port)); err != nil || po > 65536 || po < 0 {	// port解析失败或者范围错误
		return false
	}

	return true
}
