/**
 * Copyright lizhaolong(https://github.com/Super-long)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Code comment are all encoded in UTF-8.*/

package Connect

import (
	"math"
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
	raft_maxraftstate_not_suitable
	parser_snapshot_file_name
	parser_raftstate_file_name
	parser_persistence_strategy
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
	case raft_maxraftstate_not_suitable:
		ans = "raft maxraftstate not suitable, Less than zero or greater than 2MB."
		break
	case parser_snapshot_file_name:
		ans = "Format error in parsing snapshot file."
		break
	case parser_raftstate_file_name:
		ans = "Format error in parsing raftstate file."
		break
	case parser_persistence_strategy:
		ans = "No such persistence strategy."
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
	for i := len(data) - 1; i >= 0; i-- { // 解析出最后一个‘:’
		if data[i] == ':' {
			index = i
			break
		}
	}
	if index == -1 { // 未解析出`:`
		return false
	}

	ip := data[:index]
	port := data[index+1:]

	ParserRes := net.ParseIP(Sbyte2str(ip))                                // 解析ip "localhost"无法被ParseIP解析
	if ParserRes == nil && strings.ToLower(Sbyte2str(ip)) != "localhost" { // 忽略大小写
		return false
	}

	if po, err := strconv.Atoi(Sbyte2str(port)); err != nil || po > 65536 || po < 0 { // port解析失败或者范围错误
		return false
	}

	return true
}

/*
 * @brief: 解析从json提取出的文件名
 * @return: 正确返回true；否则false,这里不区分各种错误类型
 * @notes: 一下根据机器不同可以配置,Golang我没有找到接口可以直接获得以下值,所以手动配置
 *	Linux下使用getconf PATH_MAX /usr 获取路径长度限制;4096
 * 	getconf NAME_MAX /usr 获取文件名称长度限制;255
 *	还有一点是我个人的要求,后缀必须是hdb,就是这么傲娇
 */
func ParserFileName(pathname string) bool {
	Length := len(pathname)

	if Length > 4096 {
		return false
	}

	index1 := 0 // 标示后缀
	index2 := 0 // 标示文件名

	for i := Length - 1; i >= 0; i-- {
		if pathname[i] == '.' {
			index1 = i
		} else if pathname[i] == '/' {
			index2 = i
			break
		}
	}
	// 不存在后缀
	if index1 == 0 {
		return false
	}

	// 检查后缀
	// a . h d b
	// 0 1 2 3 4
	if Length-index1 != 4 {
		return false
	} else { // 简单有效 不玩花的
		if pathname[index1+1] != 'h' || pathname[index1+2] != 'd' || pathname[index1+3] != 'b' {
			return false
		}
	}

	// 检查文件名; 255见函数注释
	if index1-index2-1 > 255 {
		return false
	}

	// TODO 目前只检查了最后一个文件名,在这里检测格式是为了早点检测出错误,因为打开这个文件时协议已经开始,倒也可以使用默认文件
	for i := index2 + 1; i < index1; i++ {
		if !isEffective(pathname[i]){
			return false
		}
	}

	return true
}

/*
 * @brief: 李浩帮忙推出的服务器连接时间间隔函数
 * @return: 毫秒，调用方不用转换
 * @notes: TODO 目前看起来并不符合预期；后面可以再改，
 */
/*
 * @example: 0 	  695  893  1005 1083 	// 可以看出前几次重试斜率还是太陡峭，后面太过平缓
			 1142 1190 1230 1265 1295
			 1322 1347 1369 1389 1408
			 1426 1442 1457 1472 1485
*/
func ReturnInterval(n int) int {
	molecular := math.Log(float64(n+1)) / math.Log(math.E)
	temp := math.Log(float64(n+2)) / math.Log(1.5)
	denominator := math.Log(temp) / math.Log(math.E)
	res := molecular / denominator
	return int(res * 1000)
}

/* TODO 表还需要再测测
 * @brief: 对不齐就很烦,go Ctrl+Alt+L 会自动把注释后推
 */
var iseffective = [128]bool{
	/*0   nul    soh    stx    etx    eot    enq    ack    bel     7*/
	false, false, false, false, false, false, false, false,
	/*8   bs     ht     nl     vt     np     cr     so     si     15*/
	false, false, false, false, false, false, false, false,
	/*16  dle    dc1    dc2    dc3    dc4    nak    syn    etb    23*/
	false, false, false, false, false, false, false, false,
	/*24  can    em     sub    esc    fs     gs     rs     us     31*/
	false, false, false, false, false, false, false, false,
	/*32  ' '    !      "      #     $     %     &     '          39*/
	false, false, false, true, true, true, true, false,
	/*40  (      )      *      +     ,     -     .     /          47*/
	false, false, false, true, true, true, true, true,
	/*48  0     1     2     3     4     5     6     7             55*/
	true, true, true, true, true, true, true, true,
	/*56  8     9     :     ;     <      =     >      ?           63*/
	true, true, true, true, false, true, false, true,
	/*64  @     A     B     C     D     E     F     G             71*/
	true, true, true, true, true, true, true, true,
	/*72  H     I     J     K     L     M     N     O             79*/
	true, true, true, true, true, true, true, true,
	/*80  P     Q     R     S     T     U     V     W             87*/
	true, true, true, true, true, true, true, true,
	/*88  X     Y     Z     [      \      ]      ^      _         95*/
	true, true, true, false, false, false, false, true,
	/*96  `      a     b     c     d     e     f     g           103*/
	false, true, true, true, true, true, true, true,
	/*104 h     i     j     k     l     m     n     o            113*/
	true, true, true, true, true, true, true, true,
	/*112 p     q     r     s     t     u     v     w            119*/
	true, true, true, true, true, true, true, true,
	/*120 x     y     z     {      |      }      ~      del      127*/
	true, true, true, false, false, false, false, false,
}

/*
 * @brief: 判断文件名是否出现错误
 */
func isEffective(ch byte) bool {
	if ch < 0 { //|| ch > 127{ 	显然不太可能
		return false
	} else {
		return iseffective[ch]
	}
}

/*
 * @brief: 解析文件中的持久化策略是否正确,目前只有三种有效的策略
 */
func checkPersistenceStrategy(strategy string) bool{
	lower := strings.ToLower(strategy)

	if lower != "everysec" && lower != "no" && lower != "always"{
		return false
	}

	return true
}