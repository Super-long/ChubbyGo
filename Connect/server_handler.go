package Connect

import (
	"HummingbirdDS/Config"
	"HummingbirdDS/KvServer"
	"HummingbirdDS/Persister"
	"HummingbirdDS/Raft"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
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
	kvserver  *KvServer.RaftKV     // 一个raftkv实体
	persister *Persister.Persister // 持久化实体
	mu        sync.Mutex           // 用于保护本结构体的变量
	// 不设置成大写没办法从配置文件中读出来
	Maxreries      int      `json:maxreries` 	// 超时重连最大数
	ServersAddress []string `json:address`   	// 读取配置文件中的其他服务器地址
	MyPort string			`json:myport` 		// 自己的端口号
}

/*
 * @brief: 读取配置文件，拿到其他服务器的地址，分别建立RPC连接
 */
func (cfg *ServerConfig) connectAll() bool {
	sem := make(semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError int32 = 0
	var TimeOut []int
	var TimeOutMutex sync.Mutex

	for i := 0; i < cfg.nservers-1; i++ {
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
					number := 0 // 后面可以搞成读取配置文件
					Timeout := 200
					for number < cfg.Maxreries {
						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}
						log.Printf("%s : Reconnecting for the %d time\n", cfg.ServersAddress[i], number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond) // 倍增重连时长
						TempClient, err := rpc.DialHTTP("tcp", cfg.ServersAddress[i])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// 继续循环就ok
							default:
								// log.Fatal(temp.Error())
								atomic.AddInt32(&HTTPError, 1)
								return
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// 没有协程会碰这个
							cfg.peers[i] = TempClient
							return
						}
					}
					// 只有循环cfg.maxreries边没有结果以后才会跑到这里
					// 也就是连接超时
					TimeOutMutex.Lock()
					defer TimeOutMutex.Unlock()
					TimeOut = append(TimeOut, index) // 为了方便打日志
					return
				}(i)
				continue
			default:
				//log.Fatal(temp.Error())
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			cfg.peers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // 失败以后释放连接
		for i := 0; i < cfg.nservers-1; i++ {
			cfg.peers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		if TimeOutLength > 0 {
			log.Println(TimeOut, ": Connect Timeout!")
		}
		return false
	} else {
		return true
	}
}

func (cfg *ServerConfig) serverRegisterFun() {
	// TODO 用反射改成一个可复用的代码，代码长度可以减半
	// 把RaftKv挂到RPC上
	kvserver := new(KvServer.RaftKV)
	err := rpc.Register(kvserver)
	if err != nil {
		panic(err.Error())
	}
	// 通过函数把mathutil中提供的服务注册到http协议上，方便调用者可以利用http的方式进行数据传输
	rpc.HandleHTTP()

	// 在特定的端口进行监听 默认采用tcp 不支持配置网络协议
	listen1, err1 := net.Listen("tcp", cfg.MyPort)
	if err1 != nil {
		panic(err.Error())
	}
	go func() {
		// 调用方法去处理http请求
		http.Serve(listen1, nil)
	}()

	// 把Raft挂到RPC上
	raft := new(Raft.Raft)
	err = rpc.Register(raft)
	if err != nil {
		panic(err.Error())
	}
	// 通过函数把mathutil中提供的服务注册到http协议上，方便调用者可以利用http的方式进行数据传输
	rpc.HandleHTTP()

	// 在特定的端口进行监听
	listen2, err2 := net.Listen("tcp", cfg.MyPort)
	if err2 != nil {
		panic(err.Error())
	}
	go func() {
		// 调用方法去处理http请求
		http.Serve(listen2, nil)
	}()
}

/*
 * @brief: 再调用这个函数的时候开始服务,
 * @return: 连接可能因为网络原因出错，所以返回可能是false
 */
func (cfg *ServerConfig) StartServer() bool {
	Config.LoadServerConfig("server_config.json", cfg)

	fmt.Println("读取的address的长度为： ", len(cfg.ServersAddress))

	cfg.serverRegisterFun()
	if ok := cfg.connectAll(); !ok {
		log.Print("connect failture!\n")
		return false
	}
	// 这里初始化的原因是connectAll以后才与其他服务器连接成功;kvserver服务已经开始
	cfg.kvserver = KvServer.StartKVServer(cfg.peers, cfg.me, cfg.persister, 0)
	return true
}

/*
 * @brief: 返回一个Config结构体，用于开始一个服务
 * @param: nservers这个集群需要的机器数，包括本身，而且每个服务器初始化的时候必须配置正确
 */
func CreatServer(nservers int) *ServerConfig {
	cfg := &ServerConfig{}

	cfg.nservers = nservers
	cfg.peers = make([]*rpc.Client, cfg.nservers-1) // 存储了自己以外的其他服务器的RPC封装
	cfg.me = GenSonyflake()                         // 全局ID需要传给raft层
	cfg.persister = Persister.MakePersister()

	return cfg
}

// --------------------------
// 使用Listener模式避免/Connect和/Config的环状引用
// 当然可能好的设计可以避免这样的问题，比如把读配置文件中的函数参数改成接口，用反射去推


