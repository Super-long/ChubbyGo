package Connect

import (
	"HummingbirdDS/BaseServer"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type ClientConfig struct {
	servers        []*rpc.Client   // 表示其他几个服务器的连接句柄
	serversIsOk    []int32         // 表示哪些服务器此时可以连接
	clk            *BaseServer.Clerk // 一个客户端的实体
	nservers       int             // 连接的服务器数
	ServersAddress []string        `json:"client_address"` // 从配置文件中读取服务器的地址
	Maxreries      int             `json:"maxreries"`      // 超时重连最大数
}

func CreateClient() *ClientConfig {
	cfg := &ClientConfig{}

	return cfg
}

/*
 * @brief: 拿到其他服务器的地址，分别建立RPC连接
 * @return: 三种返回类型：超时;HttpError;成功
 * @notes: 	客户端也采取重传是因为担心在服务部署的时候直接连接导致失败;可能服务器集群此时运行时不足N个，但是仍在提供服务，所以这里逻辑与服务器并不一样
 */

/*
 * 10月23日: 以下问题已修改
 * 测试的时候发现一个问题，就是当三台服务器宕机一台的时候，客户端这么写就连接不上了，所以需要修改这里，使得多于N/2的时候可以连接成功，小于的时候服务已经下线，不必连接。
 * 这个问题并不简单，因为如果在connectAll中没有连接，到了客户端代码中又要一直操作，这势必是要加锁的。我们可以引入一个标记map标记peers的哪一项可以使用，
 * ConnectAll中在客户端服务已经启动以后还在与未连接的服务器尝试连接，连接成功以后就要修改map，使客户端代码可以连接这个新的服务器，
 * 这个map是ClientConfig和Clerk应该是共享的。‘
 * 这样看来在客户端必须一直尝试重新连接服务器，而不是和服务器一样的倍增，因为失败以后客户端不会再重连了，这与预期不符
 * 暂时不动这里，因为服务器没有宕机时连接是ok的，先把测试代码过了以后再优化这里，防止这里出错导致排错困难
 */

/*
 * 1. 最终决定使用一组数字来判断哪些peers可以重连，因为每一项peer最多只会被修改一次，
 * 对于peer的使用来说 一般这样使用
 * if atomic.load(flag) == 1{
 *	  peer.call
 * }
 * 当判断为false的时候不会操作，为true的时候peer已经可以操作了
 * 2. 目前对于宕机服务器的态度就是重连，但是这是基于大多数服务器已经连接成功以后，我们可以随意重连，
 * 3. 问题的关键在于可能大多数机器无法连接成功，此时全部的连接都应该断掉，并直接退出。
 * 	  假设有三台服务器，目前成功连接一台，那么此时会阻塞，等待连接成功，但其实我们这个时候没办法知道这两个服务器什么时候能够连接成功
 *    那么我们只能预测了。暂定的解决方案是两台服务器重试Maxreries次，肯定有一台先重试完，继续重试，但做一个标记。
 *    等到第二台机器也重试了Maxreries次以后两台一起退出，同时关闭已连接成功的那台服务器。如果中间有一台成功，那么客户端开始服务，此时剩下一台应该不停的重连
 */

// TODO 这个函数的功能集成的太过于复杂，不过目前看来没有什么好的办法
func (cfg *ClientConfig) connectAll() error {

	sem := make(Semaphore, cfg.nservers-1)
	sem_number := 0
	var HTTPError	 	int32 = 0						// HTTPError可能出现的次数
	var TimeOut 		[]int							// 超时的服务器数量
	var TimeoutMutex 	sync.Mutex						// 保护Timeout
	var SucessConnect 	int32 = 0						// 在此函数运行期间成功连接数，显然大于等于target才会退出
	var StartServer 	uint32 = 0						// 用于此函数退出以后仍在运行的协程，当值大于0时永久运行
	servers_length := 	len(cfg.ServersAddress)			// 需要连接的服务器数
	var target int32 = 	int32(servers_length/2 + 1)		// 在 target个服务器连接成功时结束

	//用于判断是否超过每一项是否超过Maxreries,其实可以复用cfg.serversIsOk,因为每一项不冲突,但是我觉得功能划分清楚一点比较好
	PeerExceedMaxreries := make([]bool, servers_length)	// 记录重传超过Maxreries次的服务器的个数

	for i := 0; i < servers_length; i++ {
		if atomic.LoadInt32(&HTTPError) > 0 { // TODO 此版本仍然不忍耐这种错误，后面尝试如何复现这种错误
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
				// 这里的重连与服务器不一样，我们需要在至少连接n/2+1时不停的重连，如果没有到达n/2+1超时的话则需要退出
				go func(index int) {
					defer sem.P(1)
					number := 0
					Timeout := 0
					// StartServer被设置为1的时候就不再看第二个判断条件了，当大多数服务器连接成功的时候StartServer被设置为1
					for atomic.LoadUint32(&StartServer) >= 1 || number <= cfg.Maxreries {

						if atomic.LoadInt32(&HTTPError) > 0 {
							return
						}

						// 至少target台服务器都经历过Maxreries次的重传
						// 可能一台服务器重试Maxreries以后重连成功，其他服务器接下来会无线重连，应该加上对StartServer的判断
						if atomic.LoadUint32(&StartServer) == 0 && judgeTrueNumber(PeerExceedMaxreries) >= target{
							break
						}

						log.Printf("%s : Reconnecting for the %d time\n", cfg.ServersAddress[index], number+1)

						number++
						if number >= cfg.Maxreries {
							number = 0
							// 不使用一个int32的原因是可能极端情况一个服务器重试了两遍Maxreries,也会退出
							PeerExceedMaxreries[index] = true
						}
						Timeout = ReturnInterval(number)

						time.Sleep(time.Duration(Timeout) * time.Millisecond)

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
							cfg.servers[index] = TempClient             // 顺序不能错，不能放到下面那一句的后面
							atomic.AddInt32(&cfg.serversIsOk[index], 1) // 客户端可能已经开始服务了，这个函数还在运行，所以要原子
							atomic.AddInt32(&SucessConnect, 1)
							log.Printf("与%d 连接成功\n", cfg.ServersAddress[i])
							return
						}
					}
					// 只有循环cfg.maxreries遍没有结果以后才会跑到这里
					// 也就是连接超时
					TimeoutMutex.Lock()
					defer TimeoutMutex.Unlock()
					TimeOut = append(TimeOut, index) // 为了方便打日志
					return
				}(i)
				continue
			default:
				atomic.AddInt32(&HTTPError, 1)
			}
		} else {
			atomic.AddInt32(&SucessConnect, 1)
			atomic.AddInt32(&cfg.serversIsOk[i], 1)	// 标记这个peer可以连接
			log.Printf("与%d 连接成功\n", cfg.ServersAddress[i])
			cfg.servers[i] = client
		}
	}

	if atomic.LoadInt32(&SucessConnect) < target {
		// 这里并没有条件竞争，因为SucessConnect只可能变大，这里在最差情况是target - SucessConnect小于零，这也没有什么关系
		sem.V(int(target - atomic.LoadInt32(&SucessConnect))) // 只要到达这个数，证明服务器集群已经可以提供服务,其他几个协程仍在连接中
	}

	TimeOutLength := len(TimeOut)
	if atomic.LoadInt32(&HTTPError) > 0 || TimeOutLength > 0 { // 失败以后释放连接
		log.Println("159 : ", atomic.LoadInt32(&HTTPError), TimeOutLength)
		for i := 0; i < servers_length; i++ {
			if atomic.LoadInt32(&(cfg.serversIsOk[i])) == 1 {
				cfg.servers[i].Close() // 连接成功的进行close
			}
		}
		if TimeOutLength > 0 {
			return ErrorInConnectAll(time_out)
		}
		return ErrorInConnectAll(http_error)
	} else {
		atomic.AddUint32(&StartServer, 1) // 代表后面的重连都是无限重连
		return nil                        // 没有发生任何错误 成功
	}
}

/*
 * @brief: 用于判断此时已经重连Maxreries的协程数，在超过阈值的时候直接退出，error为超时
 * @return: 返回数组中true的个数
 */
func judgeTrueNumber(array []bool) int32 {
	arrayLength := len(array)
	var res int32 = 0
	for i := 0; i < arrayLength; i++ {
		if array[i] {
			res++
		}
	}
	return res
}

/*
 * @brief: 再调用这个函数的时候开始服务,
 * @return: 三种返回类型：路径解析错误;connectAll连接出现问题;成功
 */
func (cfg *ClientConfig) StartClient() error {
	var flag bool = false
	if len(ClientListeners) == 1 {
		// 正确读取配置文件
		flag = ClientListeners[0]("Config/client_config.json", cfg)
		if !flag {
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
		cfg.serversIsOk = make([]int32, cfg.nservers)
	} else {
		log.Println("ClientListeners Error!")
		// 这种情况只有在调用服务没有启动read_client_config.go的init函数时会出现
		return ErrorInStartServer(Listener_error)
	}

	if err := cfg.connectAll(); err != nil {
		log.Println(err.Error())
		return ErrorInStartServer(connect_error)
	}
	cfg.clk = BaseServer.MakeClerk(cfg.servers, &(cfg.serversIsOk))
	return nil
}

/*
 * @brief: 检查从json中解析的字段是否符合规定
 * @return: 解析正确返回true,错误为false
 */
func (cfg *ClientConfig) checkJsonParser() error {
	// 当配置数小于7的时候，留给其他服务器启动的时间太少，只有八秒左右的时间
	// 这个值同样定义了客户端连接超时时间
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

	return nil
}

// --------------------------
// DEBUG用的两个函数,发现雪花算法巨大的问题,同一进程内生成的一样,导致测试出现问题

func (cfg *ClientConfig) GetUniqueFlake() uint64 {
	return cfg.clk.ClientID
}

func (cfg *ClientConfig) SetUniqueFlake(value uint64) {
	cfg.clk.ClientID = value
}

// --------------------------

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
