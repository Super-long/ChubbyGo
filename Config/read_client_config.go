package Config

import (
	"HummingbirdDS/Connect"
	"encoding/json"
	io "io/ioutil"
	"log"
	"sync"
)

func init(){
	Connect.RegisterRestClientListener(LoadClientConfig)
}

var Client_file_locker sync.Mutex //config file locker

func LoadClientConfig(filename string, cfg *Connect.ClientConfig) bool {
	Server_file_locker.Lock()
	data, err := io.ReadFile(filename) //read config file
	Server_file_locker.Unlock()
	if err != nil {
		log.Println("read json file error")
		return false
	}
	// Unmarshal对于结构体偏向于精确匹配，也接收不精确匹配
	err = json.Unmarshal([]byte(data), &cfg)
	if err != nil {
		log.Println("unmarshal json file error")
		return false
	}
	return true
}