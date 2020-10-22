package Connect

import (
	"HummingbirdDS/KvServer"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConfig struct {
	servers        []*rpc.Client   // 表示其他几个服务器的连接句柄
	clk            *KvServer.Clerk // 一个客户端的实体
	nservers       int             // 连接的服务器数
	ServersAddress []string        	`json:"client_address"` 	// 从配置文件中读取服务器的地址
	TimeOutEntry   int				`json:"timeout_entry"`		// connectAll中定义的重传超时间隔 单位为毫秒
	Maxreries      int      		`json:"maxreries"` 			// 超时重连最大数
}

func CreateClient() *ClientConfig {
	cfg := &ClientConfig{}

	return cfg
}

/*
 * @brief: 拿到其他服务器的地址，分别建立RPC连接
 * @return: 三种返回类型：超时;HttpError;成功
 * @notes: 	客户端也采取重传是因为担心在服务部署的时候直接连接导致失败;
			这里要使用与ServerConfig中connectAll相同代码的原因是考虑到可能后面要把client迁移出去，所以不必进行代码复用
 */

// TODO 测试的时候发现一个问题，就是当三台服务器宕机一台的时候，客户端这么写就连接不上了，所以需要修改这里，使得多于N/2的时候可以连接成功，小于的时候服务已经下线，不必连接。
// TODO 这个问题并不简单，因为如果在connectAll中没有连接，到了客户端代码中又要一直操作，这势必是要加锁的。我们可以引入一个标记map标记peers的哪一项可以使用，
// TODO ConnectAll中在客户端服务已经启动以后还在与未连接的服务器尝试连接，连接成功以后就要修改map，使客户端代码可以连接这个新的服务器，
// TODO 这个map是ClientConfig和Clerk共享的。
// TODO 这样看来在客户端必须一直尝试重新连接服务器，而不是和服务器一样的倍增，因为失败以后客户端不会再重连了，这与预期不符
// TODO 暂时不动这里，因为服务器没有宕机时连接是ok的，先把测试代码过了以后再优化这里，防止这里出错导致排错困难

func (cfg *ClientConfig) connectAll() error {

	sem := make(semaphore, cfg.nservers-1)
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
				// 1. 网络出现问题我们有理由报错重试，次数上限为MAXRERRIES，每次间隔时间翻倍
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := cfg.TimeOutEntry
					for number < cfg.Maxreries {
						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}
						log.Printf("%s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)
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
							cfg.servers[index] = TempClient
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
			log.Printf("与%d 连接成功\n",cfg.ServersAddress[i])
			cfg.servers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // 失败以后释放连接
		for i := 0; i < servers_length; i++ {
			cfg.servers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		return nil	// 没有发生任何错误 成功
	}
}

/*
 * @brief: 再调用这个函数的时候开始服务,
 * @return: 三种返回类型：路径解析错误;connectAll连接出现问题;成功
 */
func (cfg *ClientConfig) StartClient() error {
	var flag bool = false
	if len(ClientListeners) == 1 {
		// 正确读取配置文件
		flag =  ClientListeners[0]("/home/lzl/go/src/HummingbirdDS/Config/client_config.json", cfg)
		if !flag{
			log.Println("File parser Error!")
			return ErrorInStartServer(parser_error)
		}

		// 从json中取出的字段格式错误
		if ParserErr := cfg.checkJsonParser(); ParserErr != nil {
			log.Println(ParserErr.Error())
			return ErrorInStartServer(parser_error)
		}


		cfg.nservers = len(cfg.ServersAddress)
		cfg.servers = make([]*rpc.Client, cfg.nservers)
	} else {
		log.Println("ClientListeners Error!")
		// 这种情况只有在调用服务没有启动read_client_config.go的init函数时会出现
		return ErrorInStartServer(Listener_error)
	}

	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.clk = KvServer.MakeClerk(cfg.servers)
	return nil
}

/*
 * @brief: 检查从json中解析的字段是否符合规定
 * @return: 解析正确返回true,错误为false
 */
func (cfg *ClientConfig) checkJsonParser() error {
	// 当配置数小于7的时候，留给其他服务器启动的时间太少，配置为8的时候，至少已经过了51秒了(2^9-2^1)
	if cfg.Maxreries <= 7 {
		return ErrorInParserConfig(maxreries_to_small)
	}

	ServerAddressLength := len(cfg.ServersAddress)

	if ServerAddressLength <= 2 { // 至少三台，客户端不需要计算自己
		return ErrorInParserConfig(serveraddress_length_to_small)
	}

	for _, AddressItem := range cfg.ServersAddress {
		if !ParserIP(AddressItem) {
			return ErrorInParserConfig(serveraddress_format_error)
		}
	}

	// TODO 看下几个函数的收敛区间 等生成图像以后看一看
	// 一个超时区间不能太大或者太小。这个区间是可以保证
	if cfg.TimeOutEntry <= 100 || cfg.TimeOutEntry >= 2000 {
		return ErrorInParserConfig(time_out_entry_error)
	}

	return nil
}

func (cfg *ClientConfig) Put(key string, value string) {
	cfg.clk.Put(key, value)
}

func (cfg *ClientConfig) Append(key string, value string) {
	cfg.clk.Append(key, value)
}

func (cfg *ClientConfig) Get(key string) string {
	return cfg.clk.Get(key)
}

// --------------------------
// 使用Listener模式避免/Connect和/Config的环状引用

type ClientListener func(filename string, cfg *ClientConfig) bool

var ClientListeners []ClientListener

func RegisterRestClientListener(l ClientListener) {
	ClientListeners = append(ClientListeners, l)
}

// --------------------------
