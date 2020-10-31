/**
 * Copyright lizhaolong(https://github.com/Super-long)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Code comment are all encoded in UTF-8.*/

package BaseServer

import (
	"log"
	"sync/atomic"
	"time"
)

/*
 * @brief: 要打开的文件路径;open只是为了可以在一个目录下创建文件,与加锁权限没有一点关系
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
				return true, &FileDescriptor{reply.ChuckSum, reply.InstanceSeq, pathname}
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
func (ck *Clerk) Create(fd *FileDescriptor, Type int, filename string) (bool, *FileDescriptor){
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
				return true, &FileDescriptor{reply.CheckSum, reply.InstanceSeq, fd.PathName + "/" + filename}
			} else if reply.Err == CreateError || reply.Err == Duplicate{
				// 对端打开文件失败
				log.Printf("INFO : Create (%s/%s) error -> [%s]\n", fd.PathName, filename, reply.Err)
				ck.seq++
				return false, nil
			}
			ck.leader++
		}
	}
}

/*
 * @param: opType为操作类型，可以为delete或者close
 */
func (ck *Clerk) Delete(pathname string, filename string, instanceseq uint64, opType int, checkSum uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &CloseArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, OpType: opType, Checksum: checkSum}

		//log.Printf("DEBUG : args.checkSum %d.\n", args.Checksum)

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
			} else if reply.Err == DeleteError || reply.Err == Duplicate{
				log.Printf("INFO : Delete (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}

/*
 * @brief: 对Fd目录下的Filename进行加锁,可以加读锁或者写锁,加锁不需要open
 * @param: 实例号和路径名来源于文件描述符;文件类型;文件名称
 * @return: 返回加锁是否成功; TODO 后面可以在clerk写两个函数，其中一个只传递一个fd，最后解析一手就ok
 */
func (ck *Clerk) Acquire(pathname string, filename string, instanceseq uint64, LockType int, checksum uint64, timeout uint32) (bool, uint64) {
	cnt := len(ck.servers)

	for {
		args := &AcquireArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, LockType: LockType, Checksum: checksum, TimeOut: timeout}

		reply := new(AcquireReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++ // 不能连接就切换
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Acquire", args, reply)
			flag := true
			if err != nil {	// TODO 客户端存在巨大的问题,没有断线重连机制
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
				log.Printf("Acquire sucess, token is %d.\n", reply.Token)
				return true, reply.Token
			} else if reply.Err == AcquireError || reply.Err == Duplicate{
				// 对端打开文件失败
				log.Printf("INFO : Acqurie (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false, 0
			}
			ck.leader++
		}
	}
}

/*
 * @brief: 对特定的文件进行解锁,需要tocken是因为标记锁的版本,防止一个锁在其定义的超时范围之外进行解锁,从而解掉其他节点持有的锁
 * @param: 路径名和文件名来源于文件描述符;instanceseq号;tocken号
 * @return: 返回解锁是否成功;
 */
func (ck *Clerk) Release(pathname string, filename string, instanceseq uint64, token uint64, checksum uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &ReleaseArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			InstanceSeq: instanceseq, FileName: filename, Token: token, CheckSum: checksum}

		log.Printf("DEBUG : Release client token is %d.\n", token)

		reply := new(ReleaseReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.Release", args, reply)
			flag := true
			if err != nil {
				log.Fatal(err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == ReleaseError || reply.Err == Duplicate{
				log.Printf("INFO : Release (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}

/*
 * @brief: 附带着目前持有的token，检测这个token是否还有效
 * @notes: 问题的关键在于检查返回无效，其实在发出数据的一刻是有效的，但是不影响正确性
 */
func (ck *Clerk) CheckToken(pathname string, filename string, token uint64) bool {
	cnt := len(ck.servers)

	for {
		args := &CheckTokenArgs{PathName: pathname, ClientID: ck.ClientID, SeqNo: ck.seq,
			 FileName: filename, Token: token}

		reply := new(CheckTokenReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.CheckToken", args, reply)
			flag := true
			if err != nil {
				log.Fatal(err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case <-time.After(200 * time.Millisecond):
			ck.leader++
			continue
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == CheckTokenError || reply.Err == Duplicate{
				log.Printf("INFO : CheckToken (%s/%s) error -> [%s]\n", pathname, filename, reply.Err)
				ck.seq++
				return false
			}
			ck.leader++
		}
	}
}