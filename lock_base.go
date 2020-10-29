package main

import (
	"HummingbirdDS/BaseServer"
	_ "HummingbirdDS/Config"
	"HummingbirdDS/Connect"
	"fmt"
	"log"
	"time"
)

// 测试分为两个板块，基础锁定操作与延迟锁定

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
		fmt.Printf("Error!\n")
	}

	filename := "text.txt"
	// 在打开的文件夹下创建文件
	ok, fileFd := clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d, checksum is %d.\n", filename,fileFd.InstanceSeq, fileFd.ChuckSum)
	} else {
		fmt.Printf("Create Error!\n")
	}

	// 删除句柄,注意句柄仅由create创建，delete删除
	ok = clientConfigs.Delete(fileFd, BaseServer.Opdelete)
	if ok {
		fmt.Printf("Delete file(%s) sucess\n", filename)
	} else {
		fmt.Printf("Delete Error!\n")
	}

	// 第二次创建文件,返回的instanceSeq为1
	ok, fileFd = clientConfigs.Create(fd, BaseServer.PermanentFile,filename)
	if ok {
		fmt.Printf("Create file(%s) sucess, instanceSeq is %d, checksum is %d.\n", filename, fileFd.InstanceSeq, fileFd.ChuckSum)
	} else {
		fmt.Printf("Create Error!\n")
	}

	// 对刚刚创建文件加锁
	ok, token := clientConfigs.Acquire(fileFd, BaseServer.ReadLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Tocken is %d\n", filename, token)
	} else {
		fmt.Printf("Acquire Error!\n")
	}

	// 显然一个节点加了读锁以后再加有点蠢
	ok , token = clientConfigs.Acquire(fileFd, BaseServer.ReadLock, 0)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Token is %d\n", filename, token)
	} else {	// 显然加了写锁以后无法加读锁
		fmt.Printf("ReadLock Error!\n")
	}

	// 删除文件的时候带上自己加锁的Token
	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) sucess.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) sucess.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	ok, token = clientConfigs.Acquire(fileFd, BaseServer.WriteLock,1000)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Token is %d\n", filename, token)
	} else {	// 显然加了写锁以后无法加读锁
		fmt.Printf("WriteLock Error!\n")
	}
	// 超时以后使用此token去请求数据时会出现问题,TODO 但还没有实现带token的请求数据

	// 2000ms以后还能再次加锁成功
	time.Sleep(2000 * time.Millisecond)

	ok, token = clientConfigs.Acquire(fileFd, BaseServer.WriteLock,0)
	if ok {
		fmt.Printf("Acquire (%s) sucess, Token is %d\n", filename, token)
	} else {	// 显然加了写锁以后无法加读锁
		fmt.Printf("WriteLock Error!\n")
	}

	ok = clientConfigs.Release(fileFd, token)
	if ok {
		fmt.Printf("release (%s) sucess.\n", filename)
	} else {
		fmt.Printf("Release Error!\n")
	}

	// 使用上面已经解锁的token 应该返回false
	ok = clientConfigs.CheckToken(fileFd.PathName, token)
	if ok {
		fmt.Printf("CheckToken error！ filename(%s)\n", fileFd.PathName)
	} else {
		fmt.Printf("CheckToken sucess!\n")
	}

	ok = clientConfigs.Delete(fileFd, BaseServer.Opdelete)
	if ok {
		fmt.Printf("Delete file(%s) sucess\n", filename)
	} else {
		fmt.Printf("Delete Error!\n")
	}

	return
}