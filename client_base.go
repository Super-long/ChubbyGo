package main

import (
	_ "HummingbirdDS/Config"
	"HummingbirdDS/Connect"
	"fmt"
	"log"
	"strconv"
)

/*
 * TODO 目前来说这个测试函数是有问题的，集群启动以后的只有第一次执行是ok的，除非每次重设clientID(全局唯一ID)
 *      因为上一次的值还在服务器中，但是第二次重启的客户端中clientID和上次一样，但seq却为0，所以会出现很多的重复值
 */

func main(){
	n := 10
	Sem := make(Connect.Semaphore, n)
	SemNumber := 0
	clientConfigs := make([]*Connect.ClientConfig,n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		}
		clientConfigs[i].SetUniqueFlake(uint64(i+n*5))	// 想多次重试OK就每次把这里的0每次递增1就ok
	}

	for i := 0; i < n; i++{
		SemNumber++
		go func(cliID int){
			defer Sem.P(1)
			for j := 0; j < 10; j++ {
				nv := "x " + strconv.Itoa(cliID) + " " + strconv.Itoa(j) + " y"
				clientConfigs[cliID].Put(strconv.Itoa(cliID),nv)
				fmt.Println(cliID," : put 成功, ", nv)
				res := clientConfigs[cliID].Get(strconv.Itoa(cliID))
				if res != nv {
					fmt.Printf("%d : expected: %s, now : %s\n",cliID, nv, res)
				} else {
					fmt.Println(cliID," : Get 成功")
				}
			}
		}(i)
	}

	Sem.V(SemNumber)

	fmt.Println("PASS!")
}