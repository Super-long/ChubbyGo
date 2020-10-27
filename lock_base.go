package main

import (
	_ "HummingbirdDS/Config"
	"HummingbirdDS/Connect"
	"fmt"
	"log"
)

func main(){
	clientConfigs := Connect.CreateClient()
	err := clientConfigs.StartClient()
	if err != nil {
		log.Println(err.Error())
	}
	clientConfigs.SetUniqueFlake(uint64(3))	// 多次测试需要手动修改这个值

	ok, fd := clientConfigs.Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd sucess, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!")
	}
	return
}