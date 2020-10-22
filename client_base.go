package main

import (
	"HummingbirdDS/Connect"
	_ "HummingbirdDS/Config"
	"fmt"
	"log"
	"strconv"
	"time"
)

func main(){
	// 目前测试一个客户端
	n := 10
	clientConfigs := make([]*Connect.ClientConfig,n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		}
	}
	fmt.Println("nihao ")
	for i := 0; i < n; i++{
		go func(cliID int){
			for j := 0; j < 10; j++ {
				nv := "x " + strconv.Itoa(cliID) + " " + strconv.Itoa(j) + " y"
				clientConfigs[cliID].Put(strconv.Itoa(cliID),nv)
				fmt.Println("put 成功")
				res := clientConfigs[cliID].Get(strconv.Itoa(cliID))
				if res != nv {
					fmt.Printf("expected: %s, now : %s\n", nv, res)
				} else {
					fmt.Println("Get 成功")
				}
			}
		}(i)
	}

	time.Sleep(10 *time.Second)
	fmt.Println("PASS!")
}