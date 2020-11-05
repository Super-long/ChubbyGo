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

/*
 * @brief: 这个功能的作用是支持秒杀，比如只有100件商品，客户端只需要执行原子递减(flag为2,new为1,old为0)就可以了。基础的逻辑是客户端发送一个key值，当然value必须可以由字符串转化为数字。
 * @param: key; old; new; flag;
 * @notes: flag为1则为正常CAS操作;flag为2则为递减new,最小为old;flag为4则为递增new,最大为old;
 */

package BaseServer

import (
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

const(
	Cas = 1
	Add = 2
	Sub = 4
)


/*
 * @brief: CAS操作只有在key对应的value值为数字时才有效
 */
func (kv *RaftKV) CompareAndSwap(args *CompareAndSwapArgs, reply *CompareAndSwapReply) error {
	// 本端还没有与其他服务器连接成功
	if atomic.LoadInt32(kv.ConnectIsok) == 0{
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	var ValueOfKey string

	// 如果找到键放在OldValue中，找不到直接返回
	if value, ok := kv.KvDictionary.ChubbyGoMapGet(args.Key); ok {
		ValueOfKey = value
	} else {
		reply.Err = ErrNoKey	// 其实还可能是其他错误原因，详情请见ChubbyGoMapGet
		return nil
	}

	// 这一步确保了value可以转换为数字,并赋值给NowValue
	NowValue, err := strconv.Atoi(ValueOfKey)
	if err != nil {
		reply.Err = ValueCannotBeConvertedToNumber
		return nil
	}

	// 当interval为零的时候默认为一般的CAS操作，比较失败以后不再尝试进行递减
	NewOperation := CASOp{Key: args.Key, ClientID: args.ClientID, Clientseq: args.SeqNo, Interval: args.New}

	// 这里的old,new不一定是正确的;那么为什么要在这里进行计算呢，原因是减少守护进程临界区的计算量
	if args.Flag & 1 != 0{	// CAS
		NewOperation.Old = strconv.Itoa(args.Old)
		NewOperation.New = strconv.Itoa(args.New)
		NewOperation.Interval = 0	// CAS时为零，其他时候都是args.new
	} else if args.Flag & 2 != 0{	// ADD
		rhs := args.Old
		if NowValue + args.New > rhs{
			reply.Err = CurrentValueExceedsExpectedValue
			return nil
		}
		NewOperation.Old = strconv.Itoa(NowValue)
		NewOperation.New = strconv.Itoa(NowValue + args.New)
		NewOperation.boundary = rhs
	} else if args.Flag & 4 != 0{	// SUB
		lhs := args.Old
		if NowValue - args.New < lhs{
			reply.Err = CurrentValueExceedsExpectedValue
			return nil
		}
		NewOperation.Old = strconv.Itoa(NowValue)
		NewOperation.New = strconv.Itoa(NowValue - args.New)
		NewOperation.boundary = lhs
	} else {	// Undefined behavior
		log.Printf("ERROR : [%d] CompareAndSwap exhibits undefined behavior.\n",kv.me)
		reply.Err = CASFlagUndefined
		return nil
	}

	// 运行在此时old,与new是有效的,但是不一定最终获取有效。在守护进程中我们先进行比较，CAS，失败以后再查看边界值进行计算，以此保证ADD和SUB的有效性

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], CompareAndSwap:key(%s), oldvalue(%s), newvalue(%s)\n",
		args.ClientID,args.Key, NewOperation.Old, NewOperation.New)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()

			log.Printf("WARNING : ClientId[%d], This CompareAndSwap(key(%s) SeqNumber(%d) dup.Seq(%d)) is repeated.\n",
				args.ClientID, args.Key, args.SeqNo, dup.Seq)

			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)
	//log.Printf("DEBUG client %d : index %d\n", kv.me, index)
	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return nil
		}

		flag := <- kv.CASNotice[args.ClientID]

		if flag {
			reply.Err = OK	// 已经按照收到的请求在字典中完成更新了
		} else {
			reply.Err = CASFailture // 由于各种原因CAS失败
		}

	}

	return nil
}

/*
 * @brief: 提供一个功能更加丰富的CAS操作,见文件顶.
 * @notes: 操作要求key对应的值必须是数字，且现在其实没有做什么保护机制，也就是CAS操作的对象可能随时被修改成不是数字的值，这样就没办法了。
 * TODO 这是后面权限的一个考虑点。
 */
func (ck *Clerk) CompareAndSwap(Key string, Old int, New int, Flag int) bool{
	cnt := len(ck.servers)

	for {
		args := &CompareAndSwapArgs{ClientID: ck.ClientID, SeqNo: ck.seq, Key : Key, Old: Old, New: New, Flag: Flag}

		reply := new(CompareAndSwapReply)

		ck.leader %= cnt

		if atomic.LoadInt32(&((*ck.serversIsOk)[ck.leader])) == 0 {
			ck.leader++
			continue
		}

		replyArrival := make(chan bool, 1)

		go func() {
			err := ck.servers[ck.leader].Call("RaftKV.CompareAndSwap", args, reply)
			flag := true
			if err != nil {
				log.Printf("ERROR : Create call error, Find the cause as soon as possible -> (%s).\n", err.Error())
				flag = false
			}
			replyArrival <- flag
		}()

		select {
		case ok := <-replyArrival:
			if ok && (reply.Err == OK) {
				ck.seq++
				return true
			} else if reply.Err == CurrentValueExceedsExpectedValue || reply.Err == CASFlagUndefined ||
				 reply.Err == ValueCannotBeConvertedToNumber || reply.Err == CASFailture  || reply.Err == Duplicate{
				// 对端CAS失败，错误类型为返回值
				// log.Printf("INFO : CAS error key(%s) old(%d) new(%d) -> [%s]\n", Key, Old, New, reply.Err)
				ck.seq++
				return false
			}

			ck.leader++

		case <-time.After(200 * time.Millisecond):
			ck.leader++
		}
	}
}