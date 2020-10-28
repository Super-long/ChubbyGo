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
	clientConfigs.SetUniqueFlake(uint64(0))	// 多次测试需要手动修改这个值

	// 打开一个文件，获得句柄
	ok, fd := clientConfigs.Open("/ls/ChubbyCell_lizhaolong")
	if ok {
		fmt.Printf("Get fd sucess, instanceSeq is %d\n", fd.InstanceSeq)
	} else {
		fmt.Printf("Error!")
	}

	// TODO 显然这个文件描述符很容易被伪造，可以根据clientID在对端加密
	//		我们可以使用句柄加锁，解锁，改变ACL，ACL目前还没有定义好
	filename := "text.txt"
	// 在打开的文件夹下创建文件
	ok, fileFd := clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d\n", filename,fileFd.InstanceSeq)
	} else {
		fmt.Printf("Create Error!")
	}

	// 删除句柄,注意句柄仅由create创建，delete删除
	ok = clientConfigs.Delete(fd, filename, BaseServer.Opdelete)
	if ok {
		fmt.Printf("Close file(%s) sucess\n", filename)
	} else {
		fmt.Printf("Close Error!")
	}

	// 第二次创建文件,返回的instanceSeq为1
	ok, fileFd = clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d\n", filename, fileFd.InstanceSeq)
	} else {
		fmt.Printf("Create Error!")
	}

	// 对刚刚创建文件加锁
	ok, tocken := clientConfigs.Acquire(fileFd, BaseServer.ReadLock)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Tocken is %d\n", filename, tocken)
	} else {
		fmt.Printf("Acquire Error!")
	}

	ok , tocken = clientConfigs.Acquire(fileFd, BaseServer.ReadLock)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Tocken is %d\n", filename, tocken)
	} else {	// 显然加了写锁以后无法加读锁
		fmt.Printf("ReadLock Error!")
	}

	return
}