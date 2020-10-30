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
)

type FileOperation struct {
	pathToFileSystemNodePointer map[string]*FileSystemNode
	root                        *FileSystemNode // 根节点
}

func InitFileOperation() *FileOperation {
	Root := &FileOperation{}

	Root.pathToFileSystemNodePointer = make(map[string]*FileSystemNode)

	Root.root = InitRoot()

	//log.Printf("DEBUG : Current pathname is %s\n", Root.root.nowPath)

	return Root
}

// 如果把这个放在Kvraft中fileSystem操作时会非常麻烦
var RootFileOperation = InitFileOperation()

// 其中重复代码很多不修改的原因很多地方类型都不一样,减少代码行数就需要反射,会降低可读性;
func (kv *RaftKV) Open(args *OpenArgs, reply *OpenReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Open", ClientID: args.ClientID, Clientseq: args.SeqNo, PathName: args.PathName}

	Notice := make(chan struct{})

	log.Printf("INFO : ClientId[%d], Open : pathname(%s))\n", args.ClientID, args.PathName)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		//log.Printf("DEBUG : args.SeqNo : %d , dup.Seq : %d\n", args.SeqNo, dup.Seq)
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Open -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
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

		reply.ChuckSum = <- kv.ClientInstanceCheckSum[args.ClientID]
		reply.InstanceSeq = <- kv.ClientInstanceSeq[args.ClientID]

		if reply.ChuckSum == NoticeErrorValue || reply.InstanceSeq == NoticeErrorValue {
			reply.Err = OpenError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Create(args *CreateArgs, reply *CreateReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	Notice := make(chan struct{})

	NewOperation := FileOp{Op: "Create", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq, LockOrFileOrDeleteType: args.FileType}

	log.Printf("INFO : ClientId[%d], Create : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Create -> pathname(%s) filename(%s)) is repeated.\n", args.ClientID, args.PathName, args.FileName)
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

		reply.CheckSum = <- kv.ClientInstanceCheckSum[args.ClientID]
		reply.InstanceSeq = <- kv.ClientInstanceSeq[args.ClientID]

		if reply.CheckSum == NoticeErrorValue || reply.InstanceSeq == NoticeErrorValue {
			reply.Err = CreateError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Delete(args *CloseArgs, reply *CloseReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Delete", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq,
		LockOrFileOrDeleteType: args.OpType, CheckSum: args.Checksum}

	log.Printf("INFO : ClientId[%d], Delete : pathname(%s) filename(%s) checksum(%d)\n", args.ClientID, args.PathName, args.FileName, args.Checksum)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Delete -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
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

		NoticeError := <- kv.ClientInstanceSeq[args.ClientID]

		// 当返回值为NoticeSucess的时候没有问题
		if NoticeError == NoticeErrorValue {
			reply.Err = DeleteError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Acquire(args *AcquireArgs, reply *AcquireReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Acquire", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq,
		LockOrFileOrDeleteType: args.LockType, CheckSum: args.Checksum, TimeOut: args.TimeOut}

	log.Printf("INFO : ClientId[%d], Acquire : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Acquire -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		reply.InstanceSeq = <- kv.ClientInstanceSeq[args.ClientID]

		// 当返回值为1的时候没有问题
		if reply.InstanceSeq == NoticeErrorValue {
			reply.Err = AcquireError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) Release(args *ReleaseArgs, reply *ReleaseReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "Release", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, InstanceSeq: args.InstanceSeq, Token: args.Token, CheckSum: args.CheckSum}

	log.Printf("INFO : ClientId[%d], Release : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(Release -> pathname(%s)) is repeated.\n", args.ClientID, args.PathName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		NoticeError := <- kv.ClientInstanceSeq[args.ClientID]

		// 当返回值为1的时候没有问题
		if NoticeError == NoticeErrorValue {
			reply.Err = ReleaseError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}

func (kv *RaftKV) CheckToken(args *CheckTokenArgs, reply *CheckTokenReply) error {

	if atomic.LoadInt32(kv.ConnectIsok) == 0 {
		reply.Err = ConnectError
		return nil
	}

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = NoLeader
		return nil
	}

	NewOperation := FileOp{Op: "CheckToken", ClientID: args.ClientID, Clientseq: args.SeqNo,
		PathName: args.PathName, FileName: args.FileName, Token: args.Token}

	log.Printf("INFO : ClientId[%d], CheckToken : pathname(%s) filename(%s)\n", args.ClientID, args.PathName, args.FileName)

	Notice := make(chan struct{})

	kv.mu.Lock()
	if dup, ok := kv.ClientSeqCache[int64(args.ClientID)]; ok {
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			log.Printf("WARNING : ClientId[%d], This request(CheckToken -> pathname(%s) filename(%s)) is repeated.\n", args.ClientID, args.PathName, args.FileName)
			reply.Err = Duplicate
			return nil
		}
	}

	index, term, _ := kv.rf.Start(NewOperation)

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

		NoticeError := <- kv.ClientInstanceSeq[args.ClientID]

		// 当返回值为1的时候没有问题
		if NoticeError == NoticeErrorValue {
			reply.Err = CheckTokenError
		}

	case <-kv.shutdownCh:
		return nil
	}

	return nil
}