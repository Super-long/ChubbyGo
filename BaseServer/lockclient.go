package BaseServer

import (
	"log"
	"sync/atomic"
	"time"
)

/*
 * @brief: 要打开的文件路径
 * @return: 返回一个文件描述符
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