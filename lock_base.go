package main

import (
	"HummingbirdDS/BaseServer"
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
	clientConfigs.SetUniqueFlake(uint64(2500))	// 多次测试需要手动修改这个值

	ok, fd := clientConfigs.Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd sucess, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!")
	}

	// TODO 显然这个文件描述符很容易被伪造，可以根据clientID在对端加密
	filename := "text.txt"
	// 在打开的文件夹下创建文件
	ok, seq := clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d\n", filename,seq)
	} else {
		fmt.Printf("Create Error!")
	}

	ok = clientConfigs.Delete(fd, filename)
	if ok {
		fmt.Printf("Close file(%s) sucess\n", filename)
	} else {
		fmt.Printf("Close Error!")
	}

	// 第二次创建文件,返回的instanceSeq为1
	ok, seq = clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d\n", filename,seq)
	} else {
		fmt.Printf("Create Error!")
	}

	return
}