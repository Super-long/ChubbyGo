package BaseServer

import "log"

// TODO 目前这个结构体存在着大量的浪费情况，后面可以抽象成两个,在守护进程中使用反射解析开;不得不使用反射
type Op struct {
	Key      string
	Value    string
	Op       string 	// 代表单个操作的字符串Get，Put，Append等一众操作
	// 这样做就使得一个客户端一次只能执行一个操作了
	ClientID uint64  	// 每个Client的ID
	Clientseq    int    // 这个ClientID上目前的操作数

	// ------------------------

	InstanceSeq	uint64	// 每次请求的InstanceSeq，用于判断请求是否过期
	LockType	int		// 锁定类型
	Name		string	// 在open时是路径名，其他时候是文件名
	// 后面还有权限控制位
}

/*
 * @brief: 为了从raft接收数据，负责把从applyCh中接收到的命令转化成数据库中的值
 * 并在接收到命令的同时通知请求上的channel用于向客户返回数据
 */
func (kv *RaftKV) acceptFromRaftDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			log.Printf("INFO : [%d] is shutting down actively.\n", kv.me)
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
				if msg.Command == nil {
					log.Printf("ERROR : [%d] accept a package, msg.Command is null.\n", kv.me)
				}
				if msg.Command != nil && msg.Index > kv.snapshotIndex {
					cmd := msg.Command.(Op)
					kv.mu.Lock()
					//	显然在是一个新用户或者新操作seq大于ClientSeqCache中的值时才执行
					if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
						//if ok {
						//	log.Printf("DEBUG : dup.Seq %d ; cmd.Clientseq %d\n", dup.Seq , cmd.Clientseq)
						//}
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
						case "Open":
							kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,}
							node ,ok := RootFileOperation.pathToFileSystemNodePointer[cmd.Name]
							var seq uint64
							if ok {	// 这里是路径名;当Open的路径存在的时候进行打开，并返回InstanceSeq
								seq = node.Open(cmd.Name)	// 返回此文件的InstanceSeq
								log.Printf("INFO : [%d] open file(%s) sucess, instanceSeq is %d.\n", kv.me, cmd.Name, seq)
								kv.ClientInstanceSeq[cmd.ClientID] = seq
							} else {
								log.Printf("INFO : [%d] Not find path(%s)!\n",kv.me, cmd.Name)
							}

						default:
							log.Printf("ERROR : [%d] receive a invalid cmd %v.\n", kv.me, cmd)
						}
						// 调试时打印这个消息
						/*if ok {
						log.Printf("INFO : [%d] accept a operation. index(%d), cmd(%s), client(%d), oldSeq(%d)->newSeq(%d)\n",
							kv.me, msg.Index, cmd.Op, cmd.ClientID, dup.Seq, cmd.Clientseq)
					}*/
					}else {
						// 这种情况会在多个客户端使用相同ClientID时出现
						log.Println("ERROR : Multiple clients have the same ID !")
						// log.Printf("错误情况 dup.Seq %d ; cmd.Clientseq %d\n", dup.Seq , cmd.Clientseq)
					}
					// msg.IsSnapshot && kv.isUpperThanMaxraftstate()
					if kv.isUpperThanMaxraftstate() {
						log.Printf("INFO : [%d] need create a snapshot. maxraftstate(%d), nowRaftStateSize(%d), clientID(%d).\n",
							kv.me, kv.maxraftstate, kv.persist.RaftStateSize(), cmd.ClientID)
						kv.persisterSnapshot(msg.Index) 	// 此index以前的数据已经打包成快照了
						// 需要解决死锁；8月24日已解决!
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
	if kv.maxraftstate <= 0 { // 小于等于零的时候不执行快照
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