package KvServer

import (
	"HummingbirdDS/Persister"
	"encoding/gob"
"HummingbirdDS/Raft"
"log"
	"net/rpc"
	"sync"
"bytes"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key      string
	Value    string
	Op       string 	// 代表单个操作的字符串Get，Put，Append
	// 这样做就使得一个客户端一次只能执行一个操作了
	ClientID uint64  	// 每个Client的ID
	Clientseq    int    // 这个ClientID上目前的操作数
}

type LatestReply struct {
	Seq   int      // 最新的序列号
	Value string // 之所以get不直接从db中取是因为取时的最新值不一定是读时的最新值，我们需要一个严格有序的操作序列
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *Raft.Raft
	applyCh chan Raft.ApplyMsg

	maxraftstate int 								// 快照的阈值
	persist       *Persister.Persister				// 用于持久化
	LogIndexNotice     		map[int]chan struct{} 	// 用于服务器与raft层同步信息

	// 需要持久化的信息
	snapshotIndex int								// 现在日志上哪一个位置以前都已经是快照了，包括这个位置
	KvDictionary            map[string]string		// 字典
	ClientSeqCache 			map[int64]*LatestReply	// 用作判断当前请求是否已经执行过

	shutdownCh chan struct{}

}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// 当前已经不是leader了，自然立马返回
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return
	}

	DPrintf("[%d]: leader %d receive rpc: Get(%q).\n", kv.me, kv.me, args.Key)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.Err = Duplicate
			reply.Value = dup.Value
			return
		}
	}

	NewOperation := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, Clientseq: args.SeqNo}
	index, term, _ := kv.rf.Start(NewOperation)

	Notice := make(chan struct{})
	kv.LogIndexNotice[index] = Notice

	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		// 可能在提交之前重选也可能提交之后重选，所以需要重新发送
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return
		}

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
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return
	}

	DPrintf("[%d]: leader %d receive rpc: PutAppend(%q => (%q,%q), (%d-%d).\n", kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.Err = Duplicate
			return
		}
	}

	// 新请求
	NewOperation := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, Clientseq: args.SeqNo}
	index, term, _ := kv.rf.Start(NewOperation)
	Notice := make(chan struct{})
	kv.LogIndexNotice[index] = Notice
	kv.mu.Unlock()

	reply.Err = OK

	select {
	case <-Notice:
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.Err = ReElection
			return
		}
	case <-kv.shutdownCh:
		return
	}
}

/*
 * @brief: 为了从raft接收数据，负责把从applyCh中接收到的命令转化成数据库中的值
 × 并在接收到命令的同时通知请求上的channel用于向客户返回数据
*/
func (kv *RaftKV) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d]: server %d is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {

				// 发送的是一个快照
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// 这里我们需要持久化一下，否则可能在快照生成之前崩溃，这些数据就丢了
					kv.persisterSnapshot(msg.Index)
					kv.mu.Unlock()
					continue
				}

				if msg.Command != nil && msg.Index > kv.snapshotIndex {
					cmd := msg.Command.(Op)
					kv.mu.Lock()
					//	显然在是一个新用户或者新操作seq大于ClientSeqCache中的值时才执行
					if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
						switch cmd.Op {
						case "Get":
							kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,
								Value:kv.KvDictionary[cmd.Key],}
						case "Put":
							kv.KvDictionary[cmd.Key] = cmd.Value
							kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,}
						case "Append":
							kv.KvDictionary[cmd.Key] += cmd.Value
							kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,}
						default:
							DPrintf("[%d]: server %d receive invalid cmd: %v\n", kv.me, kv.me, cmd)
							panic("invalid command operation")
						}
						if ok {
							DPrintf("[%d]: server %d apply index: %d, cmd: %v (client: %d, dup seq: %d < %d)\n",
								kv.me, kv.me, msg.Index, cmd, cmd.ClientID, dup.Seq, cmd.Clientseq)
						}
					}
					// msg.IsSnapshot && kv.isUpperThanMaxraftstate()
					if kv.isUpperThanMaxraftstate() {
						DPrintf("[%d]: server %d need generate snapshot @ %d (%d vs %d), client: %d.\n",
							kv.me, kv.me, msg.Index, kv.maxraftstate, kv.persist.RaftStateSize(), cmd.ClientID)
						kv.persisterSnapshot(msg.Index) 	// 此index以前的数据已经打包成快照了
						// 需要解决死锁；10月2日已解决!
						kv.rf.CreateSnapshots(msg.Index)	// 使协议层进行快照
					}

					// 通知服务端操作
					if notifyCh, ok := kv.LogIndexNotice[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.LogIndexNotice, msg.Index)
					}
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *RaftKV) isUpperThanMaxraftstate() bool {
	if kv.maxraftstate < 0 { // 小于-1的时候不执行快照
		return false
	}
	// 后者其实存储的是Raft的状态大小，这个大小在Raft库中是在每次持久化时维护的
	if kv.maxraftstate < kv.persist.RaftStateSize(){
		return true
	}
	// 以上两种是极端情况，我们需要考虑靠近临界值时就持久化快照，暂定15%
	var interval = kv.maxraftstate - kv.persist.RaftStateSize()
	if interval < kv.maxraftstate/20 * 3{
		return true
	}
	return false
}

func (kv *RaftKV) persisterSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.KvDictionary)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.ClientSeqCache)

	data := w.Bytes()
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

func (kv *RaftKV) Kill() {
	close(kv.shutdownCh)
	kv.rf.Kill()

}

func StartKVServer(servers []*rpc.Client, me int, persister *Persister.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan Raft.ApplyMsg)

	kv.KvDictionary = make(map[string]string)
	kv.LogIndexNotice = make(map[int]chan struct{})
	kv.persist = persister

	kv.shutdownCh = make(chan struct{})

	kv.ClientSeqCache = make(map[int64]*LatestReply)

	// 开始的时候读取快照
	kv.readSnapshot(kv.persist.ReadSnapshot())
	//
	kv.rf = Raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyDaemon()

	return kv
}
