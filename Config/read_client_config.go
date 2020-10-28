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