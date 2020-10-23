package main

import (
	_ "HummingbirdDS/Config"
	"HummingbirdDS/Connect"
	"fmt"
	"log"
	"strconv"
)

func main(){
	// 目前测试一个客户端
	// TODO 多客户端目前出现问题 一半的数据出现问题
	n := 2
	Sem := make(Connect.Semaphore, n)
	SemNumber := 0
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
		SemNumber++
		go func(cliID int){
			defer Sem.P(1)
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

	Sem.V(SemNumber)

	fmt.Println("PASS!")
}