package BaseServer

import (
	"log"
	"sync/atomic"
	"time"
)

/*
 * @brief: 要打开的文件路径
 * @return: 返回一个文件描述符
 * @notes: 显然打开一个文件毫无意义,对于文件的操作有锁，对于内容的操作使用绝对路径为key直接get就ok
 */
func (ck *Clerk) Open(pathname string) (bool,*FileDescriptor) {
	cnt := len(ck.servers)

	for {
		args := &OpenArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(OpenReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // 不能连接就切换
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Open", args, reply)
			flag := true
			if err != nil {
				log.Fatal(err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true, &FileDescriptor{reply.InstanceSeq, pathname}
			} else if reply.Err == OpenError || reply.Err == Duplicate{
				// 对端打开文件失败
				log.Printf("INFO : Open file(%s) error -> [%s]\n", pathname, reply.Err)
				ck.seq++
				return false, nil
			}
			ck.leader++
		}
	}
}

/*
 * @brief: 在此文件描述符下创建一个文件,有三种类型目录,临时文件,文件
 * @param: 实例号和路径名来源于文件描述符;文件类型;文件名称
 * @return: 返回创建文件是否成功
 * @notes: 对于返回值要先判断bool值再判断seq,bool为falseseq是没有意义的
 */
func (ck *Clerk) Create(fd *FileDescriptor, Type int, filename string) (bool, uint64){
	cnt := len(ck.servers)

	for {
		args := &CreateArgs{PathName: fd.PathName, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: fd.InstanceSeq, FileType: Type, FileName: filename}

		reply := new(CreateReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // 不能连接就切换
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Create", args, reply)
			flag := true
			if err != nil {
				log.Fatal(err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true, reply.InstanceSeq
			} else if reply.Err == CreateError || reply.Err == Duplicate{
				// 对端打开文件失败
				log.Printf("INFO : Create (%s/%s) error -> [%s]\n", fd.PathName, filename, reply.Err)
				ck.seq++
				return false, 0
			}
			ck.leader++
		}
	}
}

func (ck *Clerk) Delete(fd *FileDescriptor, filename string) bool {
	cnt := len(ck.servers)

	for {
		args := &CloseArgs{PathName: fd.PathName, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: fd.InstanceSeq, FileName: filename}

		reply := new(CloseReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // 不能连接就切换
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Delete", args, reply)
			flag := true
			if err != nil {
				log.Fatal(err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond): // rpc timeout: 200ms
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == CreateError || reply.Err == Duplicate{
				// 对端打开文件失败
				log.Printf("INFO : Close (%s/%s) error -> [%s]\n", fd.PathName, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}