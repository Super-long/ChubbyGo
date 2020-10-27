package BaseServer

import (
	"log"
	"sync/atomic"
)

type FileOperation struct {
	pathToFileSystemNodePointer map[string]*FileSystemNode
	root *FileSystemNode	// 根节点
}

func InitFileOperation() *FileOperation{
	Root := &FileOperation{}

	Root.pathToFileSystemNodePointer = make(map[string]*FileSystemNode)

	Root.root = InitRoot()

	log.Printf("DEBUG : Current pathname is %s\n", Root.root.nowPath)

	return Root
}

// 如果把这个放在Kvraft中fileSystem操作时会非常麻烦
var RootFileOperation = InitFileOperation()

// 其中重复代码很多不修改的原因很多地方类型都不一样,减少代码行数就需要反射,会降低可读性;
func (kv *RaftKV) Open(args *OpenArgs, reply *OpenReply) error{

	if atomic.LoadInt32(kv.ConnectIsok) == 0{
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.Err = Duplicate
			return nil
		}
	}

	NewOperation := Op{Op: "Open", ClientID: args.ClientID, Clientseq: args.SeqNo, Name: args.PathName}

	log.Printf("INFO : ClientId[%d], Open : pathname(%s))\n", args.ClientID, args.PathName)

	index, term, _ := kv.rf.Start(NewOperation)
	//log.Printf("DEBUG client %d : index %d\n", kv.me, index)

	Notice := make(chan struct{})
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
		kv.mu.Lock()

		seq, ok := kv.ClientInstanceSeq[args.ClientID]
		if ok{
			reply.InstanceSeq = seq
		} else {
			reply.Err = OpenError
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
		return nil
	}

	return nil
}