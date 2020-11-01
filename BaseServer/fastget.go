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
 * @brief: FastGet请求不提供一致性保证，直接从主服务器的的字典中取值，不经过raft层;
 * @notes: FastGet请求适合客户端对一个特定的键修改以后很长时间不会修改，所有客户端可以直接get，此时fastget效率最高
 */

func (ck *Clerk) FastGet(key string) string {
	serverLength := len(ck.servers)

	for {
		args := &GetArgs{Key: key, ClientID: ck.ClientID, SeqNo: ck.seq}
		reply := new(GetReply)

		ck.leader %= serverLength
		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue // 不能连接就切换
		}

		replyArrival := make(chan bool, 1)
		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.FastGet", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : FastGet call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()
		select {
		case ok := <-replyArrival:
			if ok {
				if reply.Err == OK || reply.Err == ErrNoKey || reply.Err == Duplicate {
					log.Printf("INFO : FastGet -> ck.ClientID(%d), reply.Err(%s), reply.Value(%s).\n",
						ck.ClientID, reply.Err, reply.Value)
					ck.seq++
					return reply.Value
				} else if reply.Err == ReElection || reply.Err == NoLeader { // 这两种情况我们需要重新发送请求 即重新选择主
					ck.leader++
				}
			} else {
				ck.leader++
			}
		case <-time.After(200 * time.Millisecond): // RPC超过200ms以后直接切服务器 一般来说信道没有问题200ms绝对够用
			ck.leader++
		}
	}
	return ""
}

func (kv *RaftKV) FastGet(args *GetArgs, reply *GetReply) error {
	if atomic.LoadInt32(kv.ConnectIsok) == 0{
		reply.Err = ConnectError
		return nil
	}

	// 当前已经不是leader了，自然立马返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	log.Printf("INFO : ClientId[%d], FastGET:key(%s)\n", args.ClientID,args.Key)

	// 可以看到这个锁仍然是瓶颈
	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(GET -> key(%s)) is repeated.\n", args.ClientID, args.Key)
			reply.Err = Duplicate
			reply.Value = dup.Value
			return nil
		}
	}
	kv.mu.Unlock()

	reply.Err = OK

	if value, ok := kv.KvDictionary.ChubbyGoMapGet(args.Key); ok {
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
		reply.Value = "" // 这样写client.go可以少一个条件语句
	}

	return nil
}