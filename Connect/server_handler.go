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
	"ChubbyGo/Flake"
	"ChubbyGo/BaseServer"
	"ChubbyGo/Persister"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 第一版的创建连接还是需要服务器之间都确定对方的IP，客户端也需要知道服务器的地址
// TODO 后面可以改成类似redis，通过消息的交互得到其他节点的存在

type ServerConfig struct {
	peers     []*rpc.Client        // 表示其他几个服务器的连接句柄
	me        uint64               // 后面改成全局唯一ID
	nservers  int                  // 表示一共有多少个服务器
	kvserver  *BaseServer.RaftKV     // 一个raftkv实体
	persister *Persister.Persister // 持久化实体
	mu        sync.Mutex           // 用于保护本结构体的变量
	// 不设置成大写没办法从配置文件中读出来
	MaxRaftState   int      	`json:"maxraftstate"`    	// raft层日志压缩上限
	Maxreries      int      	`json:"maxreries"`       	// 超时重连最大数
	ServersAddress []string 	`json:"servers_address"` 	// 读取配置文件中的其他服务器地址
	MyPort         string   	`json:"myport"`          	// 自己的端口号
	TimeOutEntry   int      	`json:"timeout_entry"`   	// connectAll中定义的重传超时间隔 单位为毫秒
	// 这三个要在解析完以后传给persister
	// 后缀hdb全称为 "Honeycomb Database Backup file" 蜂巢数据备份文件
	SnapshotFileName	string	`json:"snapshotfilename"`	// 快照的持久化文件名
	RaftstateFileName	string	`json:"raftstatefilename"`	// raft状态的持久化名
	PersistenceStrategy string	`json:"persistencestrategy"`// 对应着三条策略 见persister.go 注意配置文件中不能拼写错误
}

/*
 * @brief: 拿到其他服务器的地址，分别建立RPC连接
 * @return:三种返回类型：超时;HttpError;成功
 */
// peers出现了条件竞争；修复，把服务启动在连接完成以后
func (cfg *ServerConfig) connectAll() error {
	sem := make(Semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError int32 = 0
	var TimeOut []int
	var timeout_mutex sync.Mutex

	servers_length := len(cfg.ServersAddress)
	for i := 0; i < servers_length; i++ {
		if atomic.LoadInt32(&HTTPError) > 0 {
			break
		}
		client, err := rpc.DialHTTP("tcp", cfg.ServersAddress[i])
		/*
		 * 这里返回值有三种情况:
		 * net.Dial返回error			： 重连
		 * http.ReadResponse返回		： HTTP报错
		 * 正常返回
		 */
		if err != nil {
			switch err.(type) {
			case *net.OpError: // 与库实现挂钩 不同步版本的标准库实现这里可能需要改动
				sem_number++
				// 网络出现问题我们有理由报错重试，次数上限为MAXRERRIES，每次间隔时间翻倍
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := cfg.TimeOutEntry
					for number < cfg.Maxreries {
						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}
						log.Printf("INFO : %s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond) // 倍增重连时长
						TempClient, err := rpc.DialHTTP("tcp", cfg.ServersAddress[index])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// 继续循环就ok
								continue
							default:
								atomic.AddInt32(&HTTPError, 1)
								return
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// 没有协程会碰这个
							log.Printf("INFO : %d 与 %s 连接成功\n", cfg.me, cfg.ServersAddress[index])
							cfg.peers[index] = TempClient
							return
						}
					}
					// 只有循环cfg.maxreries遍没有结果以后才会跑到这里
					// 也就是连接超时
					timeout_mutex.Lock()
					defer timeout_mutex.Unlock()
					TimeOut = append(TimeOut, index) // 为了方便打日志
					return
				}(i)
				continue
			default:
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			log.Printf("INFO : %d 与 %s 连接成功\n", cfg.me, cfg.ServersAddress[i])
			cfg.peers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // 失败以后释放连接
		for i := 0; i < servers_length; i++ {
			cfg.peers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		return nil // 没有发生任何错误 成功
	}
}

/*
 * @brief: 把raft和kvraft的挂到RPC上
 */
func (cfg *ServerConfig) serverRegisterFun() {

	// 把RaftKv挂到RPC上
	err := rpc.Register(cfg.kvserver)
	if err != nil {
		// RPC会把全部函数中满足规则的函数注册，如果存在不满足规则的函数则会返回err
		log.Println(err.Error())
	}

	// 把Raft挂到RPC上
	err1 := rpc.Register(cfg.kvserver.GetRaft())
	if err1 != nil {
		log.Println(err1.Error())
	}

	// 通过函数把mathutil中提供的服务注册到http协议上，方便调用者可以利用http的方式进行数据传输
	rpc.HandleHTTP()

	// 在特定的端口进行监听
	listen, err2 := net.Listen("tcp", cfg.MyPort)
	if err2 != nil {
		log.Println(err2.Error())
	}
	go func() {
		// 调用方法去处理http请求
		http.Serve(listen, nil)
	}()
}

/*
 * @brief: 检查从json中解析的字段是否符合规定
 * @return: 解析正确返回true,错误为false
 */
func (cfg *ServerConfig) checkJsonParser() error {
	// 当配置数小于7的时候，留给其他服务器启动的时间太少，配置为8的时候，至少已经过了51秒了(2^9-2^1)
	if cfg.Maxreries <= 7 {
		return ErrorInParserConfig(maxreries_to_small)
	}

	ServerAddressLength := len(cfg.ServersAddress)

	if ServerAddressLength < 2 { // 至少三台，且推荐为奇数,计算时加上自己
		return ErrorInParserConfig(serveraddress_length_to_small)
	}

	for _, AddressItem := range cfg.ServersAddress {
		if !ParserIP(AddressItem) {
			return ErrorInParserConfig(serveraddress_format_error)
		}
	}

	if !parserMyPort(cfg.MyPort) {
		return ErrorInParserConfig(parser_port_error)
	}

	// TODO 看下几个函数的收敛区间
	// 一个超时区间不能太大或者太小。这个区间是可以保证
	if cfg.TimeOutEntry <= 100 || cfg.TimeOutEntry >= 2000 {
		return ErrorInParserConfig(time_out_entry_error)
	}

	// /proc/cpuinfo https://blog.csdn.net/wswit/article/details/52665413 l2 cache
	if cfg.MaxRaftState < 0 || cfg.MaxRaftState > 2097152 {
		return ErrorInParserConfig(raft_maxraftstate_not_suitable)
	}

	// 解析SnapshotFileName是否符合规范
	if !ParserFileName(cfg.SnapshotFileName){
		return ErrorInParserConfig(parser_snapshot_file_name)
	}

	// 解析RaftstateFileName是否符合规范
	if !ParserFileName(cfg.RaftstateFileName){
		return ErrorInParserConfig(parser_raftstate_file_name)
	}

	// 解析PersistenceStrategy是否符合规范
	if !checkPersistenceStrategy(cfg.PersistenceStrategy){
		return ErrorInParserConfig(parser_persistence_strategy)
	}

	return nil
}

/*
 * @brief: 再调用这个函数的时候开始服务,
 * @return: 三种返回类型：路径解析错误;connectAll连接出现问题;成功
 */
func (cfg *ServerConfig) StartServer() error {
	var flag bool = false
	if len(ServerListeners) == 1 {
		// 正确读取配置文件;这里注意,checkJsonParser是检查范围和字符串的格式,解析的过程会把int转string之间的错误解析出来
		flag = ServerListeners[0]("Config/server_config.json", cfg)
		if !flag { // 文件打开失败
			log.Printf("Open config File Error!")
			return ErrorInStartServer(parser_error)
		}

		// 从json中取出的字段格式错误
		if ParserErr := cfg.checkJsonParser(); ParserErr != nil {
			log.Println(ParserErr.Error())
			return ErrorInStartServer(parser_error)
		}

		// 填充cfg.Persister
		cfg.fillPersister()

	} else {
		log.Printf("ERROR : [%d] ServerListeners Error!\n", cfg.me)
		// 这种情况只有在调用服务没有启动read_server_config.go的init函数时会出现
		return ErrorInStartServer(Listener_error)
	}

	// 这里初始化的原因是要让注册的结构体是后面运行的结构体
	cfg.kvserver = BaseServer.StartKVServerInit(cfg.me, cfg.persister, cfg.MaxRaftState)
	cfg.kvserver.StartRaftServer(&cfg.ServersAddress)

	cfg.serverRegisterFun()
	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.kvserver.StartKVServer(cfg.peers) // 启动服务
	log.Printf("INFO : [%d] The connection is successful and the service has started successfully!\n", cfg.me)
	return nil
}

/*
 * @brief: 返回一个Config结构体，用于开始一个服务
 * @param: nservers这个集群需要的机器数，包括本身，而且每个服务器初始化的时候必须配置正确
 */
func CreatServer(nservers int) *ServerConfig {
	cfg := &ServerConfig{}

	cfg.nservers = nservers
	cfg.MaxRaftState = 0
	cfg.peers = make([]*rpc.Client, cfg.nservers-1) // 存储了自己以外的其他服务器的RPC封装
	cfg.me = Flake.GetSonyflake()                   // 全局ID需要传给raft层
	cfg.persister = Persister.MakePersister()

	return cfg
}

// --------------------------
// 使用Listener模式避免/Connect和/Config的环状引用
// 当然可能好的设计可以避免这样的问题，比如把读配置文件中的函数参数改成接口，用反射去推

type ServerListener func(filename string, cfg *ServerConfig) bool

var ServerListeners []ServerListener

func RegisterRestServerListener(l ServerListener) {
	ServerListeners = append(ServerListeners, l)
}

// --------------------------

/*
 * @brief: 把从json中解出的SnapshotFileName,RaftstateFileName,PersistenceStrategy三项填充到Persister
 * @notes: 这个函数建立在已执行完checkJsonParser之后，也就是其中的值应该都满足我们的要求
 */
func (cfg *ServerConfig) fillPersister(){
	cfg.persister.RaftstateFileName = cfg.RaftstateFileName
	cfg.persister.SnapshotFileName = cfg.SnapshotFileName

	// 配置文件中策略这一项忽略大小写
	LowerStrategy := strings.ToLower(cfg.PersistenceStrategy)

	if LowerStrategy == "everysec" {
		cfg.persister.PersistenceStrategy = Persister.Everysec
	} else if LowerStrategy == "no" {
		cfg.persister.PersistenceStrategy = Persister.No
	} else {
		cfg.persister.PersistenceStrategy = Persister.Always
	}
}