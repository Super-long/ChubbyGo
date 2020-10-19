package Connect

import (
	"HummingbirdDS/KvServer"
	"log"
	"net/rpc"
	"sync/atomic"
	"net"
	"time"
)

type ClientConfig struct{
	servers []*rpc.Client			// 表示其他几个服务器的连接句柄
	clk *KvServer.Clerk				// 一个客户端的实体
	nservers int 					// 连接的服务器数
	maxreries int					// 连接的重试最大数
}

func CreateClient() *ClientConfig{
	cfg := &ClientConfig{}

	return cfg
}

// 这里要使用与ServerConfig中connectAll相同代码的原因是考虑到可能后面要把client迁移出去，所以不必进行代码复用
func (cfg *ClientConfig) connectAll() bool{
	var servers_ip []string
	// TODO 应该读取配置文件
	servers_ip = append(servers_ip, "localhost:8900", "localhost:8901","localhost:8902")
	sem := make(semaphore, cfg.nservers-1)
	sem_number := 0
	var OpErrorNumber int32 = 0
	for i := 0; i < cfg.nservers-1; i++ {
		if atomic.LoadInt32(&OpErrorNumber) > 0{
			break
		}
		client, err := rpc.DialHTTP("tcp", servers_ip[i])
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
						log.Printf("%s : Reconnecting for the %d time\n",servers_ip[i] ,number+1)
						number++
						Timeout = Timeout * 2
						time.Sleep(time.Duration(Timeout) * time.Millisecond)
						TempClient, err := rpc.DialHTTP("tcp", servers_ip[i])
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
							cfg.servers[i] = TempClient
							return
						}
					}
					return	// 只有循环cfg.maxreries边没有结果以后才会跑到这里
				}()
			}
		} else {
			cfg.servers[i] = client
		}
	}
	// 保证所有的goroutinue跑完以后退出，即要么全部连接成功，要么报错
	sem.V(sem_number)

	if atomic.LoadInt32(&OpErrorNumber) > 0{	// 失败以后释放连接
		for i := 0; i < cfg.nservers-1; i++{
			cfg.servers[i].Close() // 就算连接已经根本没建立进行close也只会返回ErrShutdown
		}
		return false
	} else {
		return true
	}
}

func (cfg *ClientConfig) StartClient() bool{
	if ok := cfg.connectAll(); !ok{
		log.Print("connect failture!\n")
		return false
	}
	cfg.clk = KvServer.MakeClerk(cfg.servers)
	return true
}

func (cfg *ClientConfig) Put(key string, value string){
	cfg.clk.Put(key, value)
}

func (cfg *ClientConfig) Append(key string, value string){
	cfg.clk.Append(key, value)
}

func (cfg *ClientConfig) Get(key string) string{
	return cfg.clk.Get(key)
}