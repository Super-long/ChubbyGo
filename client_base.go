package main

import (
	_ "ChubbyGo/Config"
	"ChubbyGo/Connect"
	"fmt"
	"log"
	"strconv"
)

/*
 * TODO 目前来说这个测试函数是有问题的，集群启动以后的只有第一次执行是ok的，除非每次重设clientID(全局唯一ID)
 *      因为上一次的值还在服务器中，但是第二次重启的客户端中clientID和上次一样，但seq却为0，所以会出现很多的重复值。
 *		目前并没有什么好的解决方案，因为这是flake算法的局限，暂时不想为测试文件重改flake
 */

func main(){
	n := 60
	Sem := make(Connect.Semaphore, n)
	SemNumber := 0
	clientConfigs := make([]*Connect.ClientConfig,n)
	flags := make([]bool, n)
	for i := 0; i < n; i++ {
		clientConfigs[i] = Connect.CreateClient()
		err := clientConfigs[i].StartClient()
		if err != nil {
			log.Println(err.Error())
		} else {	// 显然连接成功以后才可以
			clientConfigs[i].SetUniqueFlake(uint64(i+n*60))	// 想多次重试OK就每次把这里的0每次递增1就ok
			flags[i] = true
		}
	}

	for i := 0; i < n; i++{
		if !flags[i]{
			continue
		}
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
	for i:=0 ;i < len(flags);i++{
		if flags[i]{	// 至少一台服务器连接成功并执行完才算是PASS
			fmt.Println("PASS!")
			return
		}
	}
}