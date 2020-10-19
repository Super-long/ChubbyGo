package Connect

import (
	"HummingbirdDS/KvServer"
	"HummingbirdDS/Persister"
	"HummingbirdDS/Raft"
	"github.com/sony/sonyflake"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

var MAXRERRIES = 10 // 在RPC连接是允许最大的重新连接数

// 第一版的创建连接还是需要服务器之间都确定对方的IP，客户端也需要知道服务器的地址
// TODO 后面可以改成类似redis，通过消息的交互得到其他节点的存在

// 通过雪花算法生成一个全局唯一的ID
func genSonyflake() uint64 {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	return id
}

type Config struct {
	peers     	[]*rpc.Client        	// 表示其他几个服务器的连接句柄
	me        	int                  	// 后面改成全局唯一ID
	nservers  	int                  	// 表示一共有多少个服务器
	kvserver  	*KvServer.RaftKV     	// 一个raftkv实体
	persister 	*Persister.Persister 	// 持久化实体
	maxreries 	int 				 	// 超时重连最大数
	mu 			sync.Mutex				// 用于保护本结构体的变量
}

/*
 * @brief: 读取配置文件，拿到其他服务器的地址，分别建立RPC连接
 */
func (cfg *Config) connectAll() bool {
	var peers_ip []string
	// TODO 应该读取配置文件
	peers_ip = append(peers_ip, "localhost:8900", "localhost:8901")

	sem := make(semaphore, cfg.nservers-1)
	sem_number := 0
	var OpErrorNumber int32 = 0
	for i := 0; i < cfg.nservers-1; i++ {
		if atomic.LoadInt32(&OpErrorNumber) > 0{
			break
		}
		client, err := rpc.DialHTTP("tcp", peers_ip[i])
		/*
		 * 这里返回值有三种情况:
		 * net.Dial返回error			： 重连
		 * http.ReadResponse返回		： HTTP报错
		 * 正常返回
		 */
		if err != nil {
			switch err.(type) {
			case *net.OpError:	// 与库实现挂钩 不同步版本的标准库实现这里可能需要改动
				//log.Fatal(temp.Error())
				atomic.AddInt32(&OpErrorNumber, 1)
				continue
			default:
				sem_number++
				// 网络出现问题我们有理由报错重试，次数上限为MAXRERRIES，每次间隔时间翻倍
				go func() {
					defer sem.P(1)
					number := 0  // 后面可以搞成读取配置文件
					Timeout := 200
					for number < cfg.maxreries {
						if atomic.LoadInt32(&OpErrorNumber) > 0{
							return
						}
						log.Printf("%s : Reconnecting for the %d time\n",peers_ip[i] ,number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond)
						TempClient, err := rpc.DialHTTP("tcp", peers_ip[i])
						if err != nil {
							switch err.(type) {
							case *net.OpError:
								// log.Fatal(temp.Error())
								atomic.AddInt32(&OpErrorNumber, 1)
								return
							default:
							}
						} else {
							// cfg.mu.Lock()
							// defer cfg.mu.Unlock()	// 没有协程会碰这个
							cfg.peers[i] = TempClient
							return
						}
					}
					return	// 只有循环cfg.maxreries边没有结果以后才会跑到这里
				}()
			}
		} else {
			cfg.peers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	if atomic.LoadInt32(&OpErrorNumber) > 0{	// 失败以后释放连接
		for i := 0; i < cfg.nservers-1; i++{
			cfg.peers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		return false
	} else {
		return true
	}
}

func (cfg *Config) serverRegisterFun() {
	// TODO 用反射改成一个可复用的代码，代码长度可以减半
	// 把RaftKv挂到RPC上
	kvserver := new(KvServer.RaftKV)
	err := rpc.Register(kvserver)
	if err != nil {
		panic(err.Error())
	}
	// 通过函数把mathutil中提供的服务注册到http协议上，方便调用者可以利用http的方式进行数据传输
	rpc.HandleHTTP()

	// 在特定的端口进行监听
	listen1, err1 := net.Listen("tcp", ":8902")
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
	listen2, err2 := net.Listen("tcp", ":8904")
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
 */
func (cfg *Config) StartServer() {
	cfg.serverRegisterFun()
	cfg.connectAll()
	// 这里初始化的原因是connectAll以后才与其他服务器连接成功;kvserver服务已经开始
	cfg.kvserver = KvServer.StartKVServer(cfg.peers, cfg.me, cfg.persister, 0)
}

/*
 * @brief: 返回一个Config结构体，用于开始一个服务
 */
func StartConnect(nservers int) *Config {
	cfg := &Config{}

	cfg.nservers = nservers
	cfg.peers = make([]*rpc.Client, cfg.nservers-1) // 存储了自己以外的其他服务器
	cfg.me = nservers - 1
	cfg.persister = Persister.MakePersister()
	cfg.maxreries = MAXRERRIES // TODO 可以改成读取配置文件

	return cfg
}
