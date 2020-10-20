package Config

import (
	"HummingbirdDS/Connect"
	"encoding/json"
	"fmt"
	io "io/ioutil"
	"log"
	"sync"
)

var Server_file_locker sync.Mutex //config file locker

func init(){
	Connect.RegisterRestServerListener(LoadServerConfig)
}
// TODO 客户端服务器的对端地址，MAXRERRIES
func LoadServerConfig(filename string, cfg *Connect.ServerConfig) bool {
	fmt.Println(filename)
	Server_file_locker.Lock()
	data, err := io.ReadFile(filename) //read config file
	Server_file_locker.Unlock()
	if err != nil {
		log.Printf("read json file error,%s\n", err.Error())
		return false
	}
	// Unmarshal对于结构体偏向于精确匹配，也接收不精确匹配
	err = json.Unmarshal(data, &cfg)
	// 太扯了 不加双引号解不出数组
	// fmt.Printf("data %s : length %d : %s : %d\n",data, len(cfg.ServersAddress), cfg.MyPort, cfg.Maxreries)
	if err != nil {
		log.Println("unmarshal json file error")
		return false
	}
	return true
}