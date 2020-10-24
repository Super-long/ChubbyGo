package Raft

import (
	"HummingbirdDS/Persister"
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool		// true为快照；false为一般command
	Snapshot    []byte		// 快照数据
	// IsSnapshot  bool		// 用于更快的解决创建快照时的死锁问题
}

// 日志项
type LogEntry struct {
	Term    int
	Command interface{}
}

// raft的三种角色
const (
	Follower  = iota
	Candidate
	Leader
)

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Raft struct {
	mu        sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     []*rpc.Client 		// RPC end points of all peers
	persister *Persister.Persister         	// Object to hold this peer's persisted state
	me        uint64                // 用于唯一标识每一台服务器
	meIndex	  int					// 对于每一个服务器来说,永远是从config中载入的地址数加1,peers的长度也总是config中载入的地址数，me的标示也就不重要了

	CurrentTerm int        			// 服务器最后一次知道的任期号（初始化为 0，持续递增）
	VotedFor    uint64        			// 在当前获得选票的候选人的 Id
	Logs        []LogEntry 			// 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	commitIndex int   				// 已知的最大的已经被提交的日志条目的索引值 和lastApplied用于提交日志
	lastApplied int   				// 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）
	nextIndex   []int 				// 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex  []int 				// 对于每一个服务器，已经复制给他的日志的最高索引值
	// 以上成员来源于论文

	commitCond  *sync.Cond 			// 用于提交日志的时候

	state             int           // 当前状态
	electionTimer     *time.Timer   // 对于每一个raft对象都需要一个时钟 在超时是改变状态 进行下一轮的选举 2A
	electionTimeout   time.Duration // 400~800ms 选举的间隔时间不同 可以有效的防止选举失败 2A
	heartbeatInterval time.Duration // 心跳超时 论文中没有规定时间 但要小于选举超时 我选择50-100ms

	resetTimer        chan struct{} // 用于选举超时

	snapshotIndex int // 这一点之前都是快照
	snapshotTerm  int // 这一点的Term

	applyCh    chan ApplyMsg // 交付数据

	shutdownCh chan struct{}

	ConnectIsok *int32	// 参考RaftKV中的解释
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}


// 得到自己的状态，并返回自己是不是leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// 得到当前日志中最新日志条目的index和term
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	index := rf.snapshotIndex + len(rf.Logs) - 1
	term := rf.Logs[index-rf.snapshotIndex].Term
	return index, term
}

/*
 * @brief: 用于持久化需要的数据
 */
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	// 该实现为流中的每个数据类型编译自定义编解码器，当使用单个编码器传输一个值流时效率最高，分摊编译成本。
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

/*
 * @brief: 读取持久化的数据
 */
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.Logs)
	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotTerm)
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期号 2A
	CandidateID  uint64 // 请求选票的候选人ID 2A
	LastLogIndex int // 候选人的最后日志条目的索引值 2A
	LastLogTerm  int // 候选人的最后日志条目的任期号 2A
}

type RequestVoteReply struct {
	CurrentTerm int  // 当前任期号,便于返回后更新自己的任期号 2A
	VoteGranted bool // 候选人赢得了此张选票时为真 2A

	IsOk bool		 // 用于告诉请求方对端的服务器是否已经启动
}

func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.VotedFor = rf.me			// 默认投票给自己
	rf.CurrentTerm += 1			// Term加1
	rf.state = Candidate		// 改变自己的状态

	args.Term = rf.CurrentTerm
	args.CandidateID = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.lastLogIndexAndTerm()
}


// RequestVote定义了Follower收到投票以后的处理逻辑
/*
 * 我们在这个函数中需要实现将请求者的日志和被请求者的日志作对比 如果当前节点的Term比候选者节点的Term大，拒绝投票
 * 1.如果当前节点的Term比候选者节点的Term大，拒绝投票
 * 2.如果当前节点的Term比候选者节点的Term小，那么当前节点转换为Follwer状态
 * 3.判断是否已经投过票
 * 4.比较最后一项日志的Term，也就是LastLogTerm，相同的话比较索引，也就是LastLogIndex，如果当前节点较新的话就不会投票，否则投票
 */
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if atomic.LoadInt32(rf.ConnectIsok) == 0{
		reply.IsOk = false
		return nil
	}

	// fmt.Printf("当前Term : %d  对端Term: %d。  %d:收到选举请求成功,当前已投票： %d \n",rf.CurrentTerm, args.Term,rf.me, rf.VotedFor)
	reply.IsOk = true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 得到收这条消息时日志的最新信息
	lastLogIdx, lastLogTerm := rf.lastLogIndexAndTerm()

	DPrintf("[%d]: rpc RV, from peer: %d, arg term: %d, my term: %d (last log idx: %d->%d, term: %d->%d),"+
		" snapshot: %d @ %d\n", rf.me, args.CandidateID, args.Term, rf.CurrentTerm, args.LastLogIndex,
		lastLogIdx, args.LastLogTerm, lastLogTerm, rf.snapshotIndex, rf.snapshotTerm)

	if args.Term < rf.CurrentTerm {
		reply.CurrentTerm = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			// 进入新Term，把票投出去
			rf.CurrentTerm = args.Term
			rf.state = Follower
			rf.VotedFor = 0
		}

		if rf.VotedFor == 0 {
			// 对比双方日志，只有这一种情况会投票：即未投票，且对方最新日志项Term高于自己的日志项时
			if (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx) ||
				args.LastLogTerm > lastLogTerm {	// 请求投票者日志新于自己
				// log.Printf("当前Term %d， 投票给 %d\n",rf.CurrentTerm, args.Term)
				rf.resetTimer <- struct{}{} // 重置选举超时

				rf.state = Follower
				rf.VotedFor = args.CandidateID
				reply.VoteGranted = true

				log.Printf("INFO : [%d]: peer %d vote to peer %d (last log idx: %d->%d, term: %d->%d)\n",
					rf.me, rf.me, args.CandidateID, args.LastLogIndex, lastLogIdx, args.LastLogTerm, lastLogTerm)
			}
		}
	}
	rf.persist()
	return nil
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	err := rf.peers[server].Call("Raft.RequestVote", args, reply)
	flag := true
	if err != nil{
		log.Println("INFO : ", err.Error())
		flag = false
	} else if !reply.IsOk{
		// 正常情况 出现在服务器集群还未全部启动之前
		log.Println("INFO : The server is not connected to other servers in the cluster.")
		flag = false
	}
	return flag
}

// 同步日志，日志项为空时可当做心跳包
type AppendEntriesArgs struct {
	Term         int        // leader的任期号
	LeaderID     uint64     // leaderID 便于进行重定向
	PrevLogIndex int        // 新日志之前日志的索引值
	PrevLogTerm  int        // 新日志之前日志的Term
	Entries      []LogEntry // 存储的日志条目 为空时是心跳包
	LeaderCommit int        // leader已经提交的日志的索引
}

type AppendEntriesReply struct {
	CurrentTerm int  // 用于更新leader本身 因为leader可能会出现分区
	Success     bool // follower如果跟上了PrevLogIndex,PrevLogTerm的话为true,否则的话需要与leader同步日志

	// 用于同步日志
	ConflictTerm int // term of the conflicting entry
	FirstIndex   int // the first index it stores for ConflictTerm

	IsOk bool
}

func (rf *Raft) turnToFollow() {
	rf.state = Follower
	rf.VotedFor = 0
}

// AppendEntries定义了follower节点收到appendentries以后的处理逻辑
/*
 * 其实一共四种情况，就是follower日志多于leader，follower日志少于leader，follower日志等于leader（最新index处Term是否相同）
 */
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if atomic.LoadInt32(rf.ConnectIsok) == 0{
		reply.IsOk = false
		return nil
	}

	// log.Println("DEBUG : 接收 AppendEntries 请求成功\n")

	reply.IsOk = true

	DPrintf("[%d]: rpc AE, from peer: %d, term: %d\n", rf.me, args.LeaderID, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.CurrentTerm = rf.CurrentTerm
		reply.Success = false
		return nil
	}
	if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
	}

	// 分区结束以后一个落后的leader或者仅仅是延迟较大
	if rf.state == Leader {
		rf.turnToFollow()
	}
	// 一般选举完成以后第一次心跳包时修改
	if rf.VotedFor != args.LeaderID {
		rf.VotedFor = args.LeaderID
	}

	// 重置选举超时
	rf.resetTimer <- struct{}{}

	// 当心跳包中的PrevLogIndex小于快照点的时候这个包可以断定是一个落后的包
	// 之所以不用最新的日志判断是因为这些日志可能是错误的，快照点是最近的一定ok的日志，也是后面执行的前提
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.Success = false
		reply.CurrentTerm = rf.CurrentTerm
		reply.ConflictTerm = rf.snapshotTerm
		reply.FirstIndex = rf.snapshotIndex
		return nil
	}

	// 去掉对于PrevLogIndex来说多余的日志，开始寻找最近的匹配点
	preLogIdx, preLogTerm := 0, 0
	if args.PrevLogIndex < len(rf.Logs)+rf.snapshotIndex {
		preLogIdx = args.PrevLogIndex
		preLogTerm = rf.Logs[preLogIdx-rf.snapshotIndex].Term
	}

	// 如果在不同的日志中的两个条目拥有相同的索引和任期号，那么他们之前的所有日志条目也全部相同
	// 根据日志匹配原则，日志匹配成功
	if preLogIdx == args.PrevLogIndex && preLogTerm == args.PrevLogTerm {
		reply.Success = true
		// truncate to known match
		// 先截掉多余的，在加上本来要加的
		rf.Logs = rf.Logs[:preLogIdx+1-rf.snapshotIndex]
		rf.Logs = append(rf.Logs, args.Entries...)
		var last = rf.snapshotIndex + len(rf.Logs) - 1

		// 每次添加日志以后判断需不需要commit
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, last)
			// 更新日志的commitIndex
			rf.commitCond.Broadcast()
		}
		// 告诉leader去更新这个副本以匹配的index
		reply.ConflictTerm = rf.Logs[last-rf.snapshotIndex].Term	// 也就是不冲突
		reply.FirstIndex = last

		if len(args.Entries) > 0 {
			DPrintf("[%d]: AE success from leader %d (%d cmd @ %d), commit index: l->%d, f->%d.\n",
				rf.me, args.LeaderID, len(args.Entries), preLogIdx+1, args.LeaderCommit, rf.commitIndex)
		} else {
			DPrintf("[%d]: <heartbeat> current loglength: %v\n", rf.me, last)
		}
	} else {
		// 不匹配，回推index，寻找和leader最近的匹配点
		// 使用了一种比论文中更快的方法
		reply.Success = false

		// 这里其实就是两种情况 follower日志少于leader或者多于leader且有冲突
		var first = 1 + rf.snapshotIndex
		reply.ConflictTerm = preLogTerm
		if reply.ConflictTerm == 0 { // leader拥有更多的日志，此时直接让leader同步就可以了
			first = len(rf.Logs) + rf.snapshotIndex
			reply.ConflictTerm = rf.Logs[first-1-rf.snapshotIndex].Term
		} else {
			i := preLogIdx - 1	// 这一点已经冲突，从上一点开始找
			for ; i > rf.snapshotIndex; i-- {
				if rf.Logs[i-rf.snapshotIndex].Term != preLogTerm {
					first = i + 1	// 从下一条日志开始发送，也就是不匹配的地方，这里我们找到了匹配的地方，也就是一次跳一个Term
					break
				}
			}
		}
		reply.FirstIndex = first
		if len(rf.Logs)+rf.snapshotIndex <= args.PrevLogIndex {
			DPrintf("[%d]: AE failed from leader %d, leader has more logs (%d > %d), reply: %d - %d.\n",
				rf.me, args.LeaderID, args.PrevLogIndex, len(rf.Logs)-1+rf.snapshotIndex, reply.ConflictTerm,
				reply.FirstIndex)
		} else {
			DPrintf("[%d]: AE failed from leader %d, pre idx/term mismatch (%d != %d, %d != %d).\n",
				rf.me, args.LeaderID, args.PrevLogIndex, preLogIdx, args.PrevLogTerm, preLogTerm)
		}
	}
	rf.persist()
	return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	err := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	flag := true
	if err != nil{
		log.Println("INFO : ", err.Error())
		flag = false
	} else if !reply.IsOk{
		log.Println("INFO : The server is not connected to other servers in the cluster.")
		flag = false
	}
	return flag
}

/*
 * @brief: raft的入口
 * @params: 传入一个命令实体
 * @ret: 返回这条命令在日志中的index，term和此节点在执行命令时是否为leader
 */
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index, term, isLeader := -1, 0, false
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		log := LogEntry{rf.CurrentTerm,command}
		rf.Logs = append(rf.Logs, log)

		index = len(rf.Logs) - 1 + rf.snapshotIndex
		term = rf.CurrentTerm
		isLeader = true

		// 只是更新自己而已
		rf.nextIndex[rf.meIndex] = index + 1
		rf.matchIndex[rf.meIndex] = index

		rf.persist()
	}
	// log.Printf("INFO : [%d] client add a new entry (index:%d-command%v)\n", rf.me, index, command)

	return index, term, isLeader
}

/*
 * @brief: consistencyCheck的处理函数
 * @params: 副本号，调用consistencyCheck的返回值
 * @ret: void
 */
func (rf *Raft) consistencyCheckReplyHandler(n int, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 消息返回是恰好遇到了切主
	if rf.state != Leader {
		return
	}
	if reply.Success {
		// 根据返回值来更新leader对于n号副本的信息
		rf.matchIndex[n] = reply.FirstIndex
		rf.nextIndex[n] = rf.matchIndex[n] + 1
		rf.updateCommitIndex() // 尝试更新commitIndex
	} else {
		// 发现一个新主，改变状态。可能发生在分区恢复的时候
		if rf.state == Leader && reply.CurrentTerm > rf.CurrentTerm {
			rf.turnToFollow()
			rf.persist()
			rf.resetTimer <- struct{}{}
			log.Printf("INFO : [%d]->leader %d found new term (heartbeat resp from peer %d), turn to follower.",
				rf.me, rf.me, n)
			return
		}

		var know, lastIndex = false, 0
		// 发现副本出现冲突
		if reply.ConflictTerm != 0 {
			// 循环结束也没有找到证明应该发送一个快照了
			for i := len(rf.Logs) - 1; i > 0; i-- {
				if rf.Logs[i].Term == reply.ConflictTerm {
					know = true	// 现存日志中存在冲突的日志
					lastIndex = i + rf.snapshotIndex
					DPrintf("[%d]: leader %d have entry %d is the last entry in term %d.",
						rf.me, rf.me, i, reply.ConflictTerm)
					break
				}
			}
			if know {
				rf.nextIndex[n] = min(lastIndex, reply.FirstIndex)
			} else {
				rf.nextIndex[n] = reply.FirstIndex
			}
		} else {
			rf.nextIndex[n] = reply.FirstIndex
		}
		// 发送一个快照
		if rf.snapshotIndex != 0 && rf.nextIndex[n] <= rf.snapshotIndex {
			DPrintf("[%d]: peer %d need snapshot, rf.nextIndex <= rf.snapshotIndex (%d < %d).\n",
				rf.me, n, rf.nextIndex[n], rf.snapshotIndex)
			rf.sendSnapshot(n)
		} else {	// 正常情况 下次心跳会自动更新
			// snapshot + 1 <= rf.nextIndex[n] <= len(rf.Logs) + snapshot
			rf.nextIndex[n] = min(max(rf.nextIndex[n], 1+rf.snapshotIndex), len(rf.Logs)+rf.snapshotIndex)
			DPrintf("[%d]: nextIndex for peer %d  => %d (snapshot: %d).\n",
				rf.me, n, rf.nextIndex[n], rf.snapshotIndex)
		}
	}
}

/*
 * @brief: 用于leader和副本同步日志
 * @params: 副本号
 * @ret: void
 */
func (rf *Raft) consistencyCheck(n int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当rf.nextIndex[n]-1 < snapshotIndex时我们直接发送快照就ok了
	pre := rf.nextIndex[n] - 1
	if pre < rf.snapshotIndex {
		rf.sendSnapshot(n)
	} else {
		var args = AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: pre,
			PrevLogTerm:  rf.Logs[pre-rf.snapshotIndex].Term,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		// 前面已经保证了所有需要拷贝的数据都在snapshotIndex之后
		if rf.nextIndex[n] < len(rf.Logs)+rf.snapshotIndex {
			args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[n]-rf.snapshotIndex:]...)
		}
		go func() {
			DPrintf("[%d]: consistency Check to peer %d.\n", rf.me, n)
			var reply AppendEntriesReply
			if rf.sendAppendEntries(n, &args, &reply) {
				rf.consistencyCheckReplyHandler(n, &reply)
			}
		}()
	}
}

/*
 * @brief: 用于leader发送心跳包
 * @params: 传入一个命令实体
 * @ret: 返回这条命令在日志中的index，term和此节点在执行命令时是否为leader
 */
func (rf *Raft) heartbeatDaemon() {
	for {
		// 仅leader有效
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		// 重置选举超时
		rf.resetTimer <- struct{}{}

		select {
		case <-rf.shutdownCh:
			return
		default:
			PeersLength := len(rf.peers)
			for i := 0; i < PeersLength; i++ {
				// 发送心跳包
				go rf.consistencyCheck(i)
			}
		}
		// 因为心跳就需要这么长时间一次，也不会被其他事情打断，所以直接sleep就ok，不需要用定时器，代码还简单
		time.Sleep(rf.heartbeatInterval)
	}
}

/*
 * @brief: 用于更新commitIndex
 * @notice: 调用时需要加锁
 */
func (rf *Raft) updateCommitIndex() {
	match := make([]int, len(rf.matchIndex))
	copy(match, rf.matchIndex)
	sort.Ints(match)

	DPrintf("[%d]: leader %d try to update commit index: %v @ term %d.\n",
		rf.me, rf.me, rf.matchIndex, rf.CurrentTerm)

	// 找所有副本的match的中位数进行提交
	target := match[len(rf.peers)/2]
	if rf.commitIndex < target && rf.snapshotIndex < target {
		if rf.Logs[target-rf.snapshotIndex].Term == rf.CurrentTerm {
			DPrintf("[%d]: leader %d update commit index %d -> %d @ term %d\n",
				rf.me, rf.me, rf.commitIndex, target, rf.CurrentTerm)
			rf.commitIndex = target
			rf.commitCond.Broadcast()
		} else {
			DPrintf("[%d]: leader %d update commit index %d failed (log term %d != current Term %d)\n",
				rf.me, rf.me, rf.commitIndex, rf.Logs[target-rf.snapshotIndex].Term, rf.CurrentTerm)
		}
	}
}

/*func (rf *Raft) Kill() {
	close(rf.shutdownCh)
	rf.commitCond.Broadcast()
}
*/

/*
 * @brief: 用于把数据提交给rf.applyCh
 * @notice: 调用时需要加锁
 */
func (rf *Raft) applyLogEntryDaemon() {
	for {
		var logs []LogEntry
		rf.mu.Lock()
		for rf.lastApplied == rf.commitIndex {	// 被唤醒的时候跳出循环
			rf.commitCond.Wait()
			select {
			case <-rf.shutdownCh:
				rf.mu.Unlock()
				DPrintf("[%d]: peer %d is shutting down apply log entry to client daemon.\n", rf.me, rf.me)
				close(rf.applyCh)
				return
			default:
			}
		}
		// last是上一个已经commit的值，实际commit区间是[last+1, cur]
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {	// 拷贝效率不高
			// 避免死锁
			rf.lastApplied = rf.commitIndex
			logs = make([]LogEntry, cur-last)
			copy(logs, rf.Logs[last+1-rf.snapshotIndex: cur+1-rf.snapshotIndex])
		}
		rf.mu.Unlock()

		// 因为在提交日志的时候kv层可能会创建快照，其中加锁，如果在向applych中加值的时候上锁就会死锁
		// 有两种方法可以解决，最简单的就是把日志拷贝一下，向apply拷贝值的时候用副本;
		// 第二种是加一个标志位IsSnapshot，在apply结束以后发送这种消息类型，kv收到才可以创建快照
		/*		for i := last+1; i <= cur; i++ {
				if i != cur{
					rf.applyCh <- ApplyMsg{Index: i, Command: rf.Logs[i-rf.snapshotIndex].Command, IsSnapshot: false}
				} else {
					rf.applyCh <- ApplyMsg{Index: i, Command: rf.Logs[i-rf.snapshotIndex].Command, IsSnapshot: true}
				}
				fmt.Printf("rf.me %d; index : %d\n",rf.me,  i)
			}*/
		for i := 0; i < cur-last; i++ {
			rf.applyCh <- ApplyMsg{Index:last + i + 1, Command: logs[i].Command}
		}
	}
}

/*
 * @brief: 用于请求投票，并在RPC成功以后处理
 */
func (rf *Raft) canvassVotes() {
	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)
	peers := len(rf.peers)

	var votes = 1
	replyHandler := func(reply *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state == Candidate {
			if reply.CurrentTerm > voteArgs.Term {
				rf.CurrentTerm = reply.CurrentTerm
				rf.turnToFollow()
				log.Printf("INFO : [%d] become new follower! \n", rf.me)
				rf.persist()
				rf.resetTimer <- struct{}{} // reset timer
				return
			}
			fmt.Println()
			if reply.VoteGranted {	// 选举成功
				votes++	// 10月22日 修改，这里竟然写错位置了！导致一节点宕机时其他两节点无法达成共识，因为对端投票这里没加，下一次又跳Term，这个投票就无效了，三小时啊
				// log.Printf("DEBUG : Term : %d ; votes : %d ; expected : %d\n",rf.CurrentTerm,votes, (peers+1)/2 + 1)
				if votes == (peers+1)/2 + 1 {	// peers比实际机器数少1，不计算自己
					rf.state = Leader
					log.Printf("INFO : [%d] become new leader! \n", rf.me)
					rf.resetOnElection()    // 重置leader状态
					go rf.heartbeatDaemon() // 选举成功以后 执行心跳协程
					return
				}
				// votes++
			}
		}
	}
	for i := 0; i < peers; i++ {
		// 这peers项全部都是对等的服务器
		go func(n int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(n, &voteArgs, &reply) {
				replyHandler(&reply)
			}
		}(i)
	}
}

/*
 * @brief: 在重新选举以后设置基础值
 * @notice: 调用时需要加锁
 */
func (rf *Raft) resetOnElection() {
	count := len(rf.peers)
	length := len(rf.Logs) + rf.snapshotIndex

	for i := 0; i < count; i++ {	// 更新其他对端服务器
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = length
	}
	rf.matchIndex[rf.meIndex] = length - 1
}

/*
 * @brief: 选举协程
 */
func (rf *Raft) electionDaemon() {
	for {
		select {
		case <-rf.shutdownCh:
			DPrintf("[%d]: peer %d is shutting down electionDaemon.\n", rf.me, rf.me)
			return
		case <-rf.resetTimer:	// 重置超时时钟
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			DPrintf("[%d]: peer %d election timeout, issue election @ term %d\n",
				rf.me, rf.me, rf.CurrentTerm)
			go rf.canvassVotes()
			// 防止第一次每台服务器随机的值差不多，造成活锁
			rf.electionTimer.Reset(time.Millisecond * time.Duration(400+rand.Intn(100)*4))
		}
	}
}

func (rf *Raft) MakeRaftServer(peers []*rpc.Client){
	rf.peers = peers
	// 多的这一项是自己,为更新其他服务器
	peerLength := len(peers)
	rf.nextIndex = make([]int, peerLength + 1)
	rf.matchIndex = make([]int, peerLength + 1)
	// 对于每一个服务器来说前peerLength项都是与其他服务器的通信实体，index为peerLength的即是自己
	rf.meIndex = peerLength

	go rf.electionDaemon()      				// 开始选举
	go rf.applyLogEntryDaemon() 				// 开始追加日志
	go Persister.PersisterDaemon(rf.persister)	// 开始根据策略写盘+刷盘

	DPrintf("[%d]: newborn election(%s) heartbeat(%s) term(%d) voted(%d)\n",
		rf.me, rf.electionTimeout, rf.heartbeatInterval, rf.CurrentTerm, rf.VotedFor)
}

/*
 * @brief: 用于创建一个raft实体
 */
// TODO 这里传入的实体很有意思，这里就已经知道了如何与其他服务器通信和持久化的具体策略，并被传入一个chan
func MakeRaftInit(me uint64,
	persister *Persister.Persister, applyCh chan ApplyMsg, IsOk *int32) *Raft {
	rf := &Raft{}
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.VotedFor = 0
	rf.Logs = make([]LogEntry, 1) // first index is 1
	rf.Logs[0] = LogEntry{// placeholder
		Term: 0,
		Command: nil,
	}

	// 400~800 ms
	rf.electionTimeout = time.Millisecond * time.Duration(400+rand.Intn(100)*4)

	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.resetTimer = make(chan struct{})
	rf.shutdownCh = make(chan struct{})          // shutdown raft gracefully
	rf.commitCond = sync.NewCond(&rf.mu)         // commitCh, a distinct goroutine
	rf.heartbeatInterval = time.Millisecond * 50 // small enough, not too small

	// TODO 需要从文件读一手
	rf.readPersist(persister.ReadRaftStateFromFile())

	rf.lastApplied = rf.snapshotIndex
	rf.commitIndex = rf.snapshotIndex

	rf.ConnectIsok = IsOk

	return rf
}

// 快照部分

/*
 * @brief: 用于在日志超过阈值时进行日志压缩，由kv层调用,index之前已经生成快照了
 */
func (rf *Raft) CreateSnapshots(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ----------------[------区间A--------]
	// ----------------|       ||         |
	// snapshotIndex---[sna+1, commitIndex];只有index落在区间A是有效的
	if rf.commitIndex < index || index <= rf.snapshotIndex {
		panic("NewSnapShot(): new.snapshotIndex <= old.snapshotIndex")
	}
	// 丢弃日志，前面的快照已经在kv层保存过了
	rf.Logs = rf.Logs[index-rf.snapshotIndex:]

	rf.snapshotIndex = index
	rf.snapshotTerm = rf.Logs[0].Term

	DPrintf("[%d]: peer %d have new snapshot, %d @ %d.\n",
		rf.me, rf.me, rf.snapshotIndex, rf.snapshotTerm)
	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int 		// 领导人的任期号
	LeaderID          uint64	// 领导人的ID，以便于跟随者重定向请求
	LastIncludedIndex int		// 快照中包含的最后日志条目的索引值
	LastIncludedTerm  int		// 快照中包含的最后日志条目的任期号
	Snapshot          []byte	// 快照数据
}

type InstallSnapshotReply struct {
	CurrentTerm int 	// leader可能已经落后了，用于更新leader

	IsOk bool
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if atomic.LoadInt32(rf.ConnectIsok) == 0{
		reply.IsOk = false
		return nil
	}
	reply.IsOk = true
	select {
	case <-rf.shutdownCh:
		DPrintf("[%d]: peer %d is shutting down, reject install snapshot rpc request.\n",
			rf.me, rf.me)
		return nil
	default:
	}

	DPrintf("[%d]: rpc snapshot, from peer: %d, term: %d\n", rf.me, args.LeaderID, args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.CurrentTerm = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		DPrintf("[%d]: rpc snapshot, args.term < rf.CurrentTerm (%d < %d)\n", rf.me,
			args.Term, rf.CurrentTerm)
		return nil
	}

	// 快照可能也会重复
	if args.LastIncludedIndex <= rf.snapshotIndex {
		DPrintf("[%d]: rpc snapshot, args.LastIncludedIndex <= rf.snapshotIndex (%d < %d)\n", rf.me,
			args.LastIncludedIndex, rf.snapshotIndex)
		return nil
	}

	rf.resetTimer <- struct{}{}

	// 当快照大于全部的日志时，用快照更新raft的全部属性
	if args.LastIncludedIndex >= rf.snapshotIndex+len(rf.Logs)-1 {

		DPrintf("[%d]: rpc snapshot, snapshot have all logs (%d >= %d + %d - 1).\n", rf.me,
			args.LastIncludedIndex, rf.snapshotIndex, len(rf.Logs))

		rf.snapshotIndex = args.LastIncludedIndex
		rf.snapshotTerm = args.LastIncludedTerm
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
		rf.Logs = []LogEntry{{rf.snapshotTerm, nil},}

		rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

		rf.persist()
		return nil
	}

	// 本地日志大于快照，只更新一部分
	DPrintf("[%d]: rpc snapshot, snapshot have some logs (%d < %d + %d - 1).\n", rf.me,
		args.LastIncludedIndex, rf.snapshotIndex, len(rf.Logs))

	// [snapshotIndex, 8, LastIncludedIndex,10 ]
	// [7,8,9,10]
	// [0,1,2,3]
	//    ||
	//  [9,10]
	rf.Logs = rf.Logs[args.LastIncludedIndex-rf.snapshotIndex:]
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.commitIndex = rf.snapshotIndex
	rf.lastApplied = rf.snapshotIndex	// 不需要一项一项发了，都设置为LastIncludedIndex就可以了

	rf.applyCh <- ApplyMsg{rf.snapshotIndex, nil, true, args.Snapshot}

	rf.persist()
	return nil
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	err := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	flag := true
	if err != nil{
		log.Println("INFO :", err.Error())
		flag = false
	} else if !reply.IsOk{
		log.Println("INFO : The server is not connected to other servers in the cluster.")
		flag = false
	}
	return flag
}

/*
 * @brief: 用于在副本数据落后于leader的snapshotIndex时进行更新
 * @param: server为副本编号
 */
func (rf *Raft) sendSnapshot(server int) {
	var args = &InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		LeaderID:          rf.me,
		Snapshot:          rf.persister.ReadSnapshot(),	// 把快照发送过去
	}
	replayHandler := func(server int, reply *InstallSnapshotReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == Leader {
			// rf是一个落后的leader
			if reply.CurrentTerm > rf.CurrentTerm {
				rf.CurrentTerm = reply.CurrentTerm
				rf.turnToFollow()
				return
			}
			// 更新其nextindex
			rf.matchIndex[server] = rf.snapshotIndex
			rf.nextIndex[server] = rf.snapshotIndex + 1
		}
	}
	go func() {
		var reply InstallSnapshotReply
		if rf.sendInstallSnapshot(server, args, &reply) {
			replayHandler(server, &reply)
		}
	}()
}