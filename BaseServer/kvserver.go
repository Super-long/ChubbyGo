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
	"ChubbyGo/Persister"
	"ChubbyGo/Raft"
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"sync"
	"sync/atomic"
)

type LatestReply struct {
	Seq   int      // 最新的序列号
	Value string // 之所以get不直接从db中取是因为取时的最新值不一定是读时的最新值，我们需要一个严格有序的操作序列
}

type fileSystemNoticeTableEntry struct {
	instacne uint64
	checksum uint64
}

type RaftKV struct {
	mu      sync.Mutex
	me      uint64
	rf      *Raft.Raft
	applyCh chan Raft.ApplyMsg

	maxraftstate int 								// 快照的阈值
	persist       			*Persister.Persister	// 用于持久化
	LogIndexNotice     		map[int]chan struct{} 	// 用于服务器与raft层同步信息

	// 需要持久化的信息
	snapshotIndex int								// 现在日志上哪一个位置以前都已经是快照了，包括这个位置
	KvDictionary            map[string]string		// 字典
	ClientSeqCache 			map[int64]*LatestReply	// 用作判断当前请求是否已经执行过

	// 以下两项均作为通知机制;注意,协商以0为无效值,这样可以避免读取时锁的使用,ClientInstanceSeq用作instance和token
	ClientInstanceSeq		map[uint64]chan uint64	// 用作返回给客户端的InstanceSeq
	// 仅用于Open操作
	ClientInstanceCheckSum	map[uint64]chan uint64	// 用作返回给客户端的CheckSum

	shutdownCh chan struct{}

	// 显然这个数的最大值就是1，也就是连接成功的时候，且只有两种情况，即0和1
	// 不使用bool的原因是Golang的atomic貌似没有像C++一样提供atomic_flag这样保证无锁的bool值
	// 如果硬用bool加锁的话又慢又不好写，因为raft和kvraft应该是共享这个值的
	ConnectIsok *int32		// 用于同步各服务器之间的服务的具体启动时间 且raft与kvraft应该使用同一个值
/*
 * ok是此进程连接上所有其他服务器的时间点，显然后ok的进程与先ok的进程可以立即通信
 * 显然在 p1 刚刚 ok 时对其他服务器进行的选举和心跳行为应该是无效的;所以所有被RPC的函数都应该先判断ConnectIsok才决定是否返回值
 * 但是其实p1的守护进程已经开启了
 * 目前的做法是在每个服务器端设置一个字段称为ConnectIsok
 * 在被远端进行RPC的时候，如果本段的连接还没有完成，就给RPC返回失败
	p1			p2			p3
	ok
				ok
							ok
*/
}

// 用于server_handler.go,注册raft服务
func (kv *RaftKV) GetRaft() *Raft.Raft{
	return kv.rf
}

// 显然对于同一个客户端不允许并发执行多个操作,会发生死锁,这是我定义的使用规范
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) error {
	// kvraft的这个也可能被触发，用于客户端连接上三个服务器，但其中一台服务器还没有连接到全部的别的服务器，此时对于这个服务器来说应该拒绝请求
	// 客户端只需要切换leader就ok了
	if atomic.LoadInt32(kv.ConnectIsok) == 0{
		reply.Err = ConnectError
		return nil
	}

	// 当前已经不是leader了，自然立马返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := KvOp{Key: args.Key, Op: "Get", ClientID: args.ClientID, Clientseq: args.SeqNo}

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], GET:key(%s)\n", args.ClientID,args.Key)

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

	// raft本身就有锁,放在临界区内的原因是我们必须要得到index,而start必须在检测clientSeqCache之后,好在Start没有阻塞函数
	index, term, _ := kv.rf.Start(NewOperation)

	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		// 可能在提交之前重选也可能提交之后重选，所以需要重新发送
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return nil
		}

		// TODO 显然只是修改了字典 可以把字典改成线程安全的哈希map，但是这样在持久化的时候就比较麻烦，为了性能，改！
		kv.mu.Lock()
		if value, ok := kv.KvDictionary[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = "" // 这样写client.go可以少一个条件语句
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
	}
	return nil
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// 本端还没有与其他服务器连接成功
	if atomic.LoadInt32(kv.ConnectIsok) == 0{
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := KvOp{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, Clientseq: args.SeqNo}
	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], PUTAPPEND:key(%s),value(%s)\n", args.ClientID,args.Key, args.Value)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()

			log.Printf("WARNING : ClientId[%d], This request(PutAppend -> key(%s) value(%s)) is repeated.\n", args.ClientID, args.Key, args.Value)
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
	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

/*
 * @notes: 调用这个函数必须在kvraft的锁保护范围之内进行
 */
func (kv *RaftKV) persisterSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.KvDictionary)	// 快照点以前的值已经被存到KvDictionary了
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.ClientSeqCache)

	data := w.Bytes()

	// TODO 这里的落盘操作是在临界区内的，是否可以修改，当然这也是不推荐always的理由
	kv.persist.SaveSnapshot(data)
}

func (kv *RaftKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.KvDictionary = make(map[string]string)
	kv.ClientSeqCache = make(map[int64]*LatestReply)

	d.Decode(&kv.KvDictionary)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.ClientSeqCache)
}

/*func (kv *RaftKV) Kill() {
	close(kv.shutdownCh)
	kv.rf.Kill()

}*/

func StartKVServerInit(me uint64, persister *Persister.Persister, maxraftstate int) *RaftKV {
	gob.Register(KvOp{})
	gob.Register(FileOp{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan Raft.ApplyMsg)

	kv.KvDictionary = make(map[string]string)
	kv.ClientInstanceSeq = make(map[uint64]chan uint64)
	kv.ClientInstanceCheckSum = make(map[uint64]chan uint64)
	kv.LogIndexNotice = make(map[int]chan struct{})
	kv.persist = persister

	kv.shutdownCh = make(chan struct{})

	kv.ClientSeqCache = make(map[int64]*LatestReply)

	var Isok int32 = 0	// 最大只能是1 因为只有在连接成功的时候会加一次
	kv.ConnectIsok = &Isok

	// 开始的时候读取快照
	kv.readSnapshot(persister.ReadSnapshotFromFile())
	// ps：很重要,因为从文件中读取的值只是字段，还没有存在persister中,只有存了以后才可以持久化成功,否则会出现宕机重启后snapshot.hdb为0
	kv.persisterSnapshot(kv.snapshotIndex)	// 这样写会导致起始snapshot.hdb文件大小为76,不过问题不大,因为必须这样做

	//log.Printf("DEBUG : [%d] snapshotIndex(%d), len(%d) .\n",kv.me ,kv.snapshotIndex, len(kv.ClientSeqCache))

	// 不写在这里会写在InitFileOperation会出现环状调用，这里的结构可以后面改一改，耦合太高
	RootFileOperation.pathToFileSystemNodePointer[RootFileOperation.root.nowPath] = RootFileOperation.root

	return kv
}

func (kv *RaftKV)StartKVServer(servers []*rpc.Client){

	atomic.AddInt32(kv.ConnectIsok, 1)	// 到这肯定已经连接上其他的服务器了

	// 启动kv的服务
	kv.rf.MakeRaftServer(servers)

	go kv.acceptFromRaftDaemon()
}

func (kv *RaftKV) StartRaftServer(address *[]string){
	kv.rf = Raft.MakeRaftInit(kv.me, kv.persist, kv.applyCh, kv.ConnectIsok, address)
}