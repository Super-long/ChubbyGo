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
	"fmt"
	"log"
	"strconv"
	"time"
)

type KvOp struct {
	Key   string
	Value string
	Op    string // 代表单个操作的字符串Get，Put，Append等一众操作
	// 这样做就使得一个客户端一次只能执行一个操作了
	ClientID  uint64 // 每个Client的ID
	Clientseq int    // 这个ClientID上目前的操作数
}

type FileOp struct {
	Op        string // 代表单个操作的字符串open,create等一众操作
	ClientID  uint64 // 每个Client的ID
	Clientseq int    // 这个ClientID上目前的操作数

	InstanceSeq uint64 // 每次请求的InstanceSeq，用于判断请求是否过期
	Token       uint64 // 锁的版本号

	LockOrFileOrDeleteType int // 锁定类型或者文件类型或者delete，反正三个不会一起用

	FileName string // 在open时是路径名，其他时候是文件名
	PathName string // 路径名称
	CheckSum uint64 // 校验位
	TimeOut  uint32 // 加锁超时参数

	// TODO 权限控制位,现在还没用,因为不确定到底该以什么形式来设置权限
	ReadAcl   *[]uint64
	WriteAcl  *[]uint64
	ModifyAcl *[]uint64
}

// 用于CAS
type CASOp struct{
	ClientID  uint64 // 每个Client的ID
	Clientseq int    // 这个ClientID上目前的操作数

	Key string
	Old string		// 思前想去old,new这里还是搞成string比较好，省的在守护线程里面再转换一次，减少临界区的开销
	New string
	boundary int	// 当在CAS比较失败的时候进行边界对比，如果还可以容纳一个interval，进行操作。
	Interval int
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

					// 这个参数的作用是把raft的快照操作从kv.mu的临界区拿出去
					var IsSnapShot bool = false
					var IsNeedSnapShot bool = kv.isUpperThanMaxraftstate()

					if cmd, ok := msg.Command.(KvOp); ok {
						kv.mu.Lock()
						//	显然在是一个新用户或者新操作seq大于ClientSeqCache中的值时才执行
						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							//if ok {
							//	log.Printf("DEBUG : dup.Seq %d ; cmd.Clientseq %d\n", dup.Seq , cmd.Clientseq)
							//}
							switch cmd.Op {
							case "Get":
								// 不需要管bool返回值是因为ChubbyGoMapGet以""做false的返回值.也符合我们的预期
								value, _ := kv.KvDictionary.ChubbyGoMapGet(cmd.Key)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq,
									Value: value}
							case "Put":
								kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.Value)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
							case "Append":
								kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.Value)
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}

							default:
								log.Printf("ERROR : [%d] receive a invalid cmd %v.\n", kv.me, cmd)
							}
							// 调试时打印这个消息
							/*if ok {
								log.Printf("INFO : [%d] accept a operation. index(%d), cmd(%s), client(%d), oldSeq(%d)->newSeq(%d)\n",
									kv.me, msg.Index, cmd.Op, cmd.ClientID, dup.Seq, cmd.Clientseq)
							}*/
						} else {
							// 这种情况会在多个客户端使用相同ClientID时出现
							log.Println("ERROR : Multiple clients have the same ID !")
							// log.Printf("错误情况 dup.Seq %d ; cmd.Clientseq %d\n", dup.Seq , cmd.Clientseq)
						}
					} else if cmd, ok := msg.Command.(FileOp); ok {
						// 这里的问题是我在临界区内打了很多日志，这其实比较蠢，但是现在先不急着改
						kv.mu.Lock()

						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							if !ok { // 此时显然这个用户是新的，我们创建一个无缓冲的channel
								// 这里为什么设置3呢，原因是设置通知这里可能出现多次进入但lockserver中还没有取
								//这种概率极低，我测试了很多次都没有出现，但是理论存在，所以设置一个3作为度
								kv.ClientInstanceCheckSum[cmd.ClientID] = make(chan uint64, 3)
								kv.ClientInstanceSeq[cmd.ClientID] = make(chan uint64, 3)
							}

							switch cmd.Op {
							case "Open":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok { // 这里是路径名;当Open的路径存在的时候进行打开，并返回InstanceSeq
									seq, chuckSum := node.Open(cmd.PathName) // 返回此文件的InstanceSeq
									node.OpenReferenceCount++
									log.Printf("INFO : [%d] Open file(%s) sucess, instanceSeq is %d, chucksum is %s.\n", kv.me, cmd.PathName, seq, chuckSum)
									kv.ClientInstanceSeq[cmd.ClientID] <- seq
									kv.ClientInstanceCheckSum[cmd.ClientID] <- chuckSum
								} else {
									// 零协议为错误的值,用作通知机制,checksum和seq中零都是错误的值
									kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
									kv.ClientInstanceCheckSum[cmd.ClientID] <- NoticeErrorValue
									log.Printf("INFO : [%d] Open Not find path(%s)!\n", kv.me, cmd.PathName)
								}
							case "Create":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									seq, checksum, err := node.Insert(cmd.InstanceSeq, cmd.LockOrFileOrDeleteType, cmd.FileName, nil, nil, nil)
									if err == nil {
										log.Printf("INFO : [%d] Create file(%s) sucess, instanceSeq is %d, checksum is %d.\n", kv.me, cmd.FileName, seq, checksum)
										kv.ClientInstanceSeq[cmd.ClientID] <- seq
										kv.ClientInstanceCheckSum[cmd.ClientID] <- checksum
										break
									} else {
										log.Printf("INFO : [%d] Create file(%s) failture, %s\n", kv.me, cmd.FileName, err.Error())
									}
								} else {
									log.Printf("INFO : [%d] Create Not find path(%s)!\n", kv.me, cmd.PathName)
								}
								// 合并上面两个条件
								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
								kv.ClientInstanceCheckSum[cmd.ClientID] <- NoticeErrorValue

							case "Delete":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									err := node.Delete(cmd.InstanceSeq, cmd.FileName, cmd.LockOrFileOrDeleteType, cmd.CheckSum)
									if err == nil {
										log.Printf("INFO : [%d] Delete file(%s) sucess.\n", kv.me, cmd.FileName)
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSucess // 特殊的情况,我们需要一个通知机制
										break
									} else {
										log.Printf("INFO : [%d] Delete file(%s) failture, %s.\n", kv.me, cmd.FileName, err.Error())
									}
								} else {
									log.Printf("INFO : [%d] Delete Not find path(%s)!\n", kv.me, cmd.PathName)
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "Acquire":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									token, err := node.Acquire(cmd.InstanceSeq, cmd.FileName, cmd.LockOrFileOrDeleteType, cmd.CheckSum)
									if err == nil {
										var LockTypeName string
										if cmd.LockOrFileOrDeleteType == WriteLock {
											LockTypeName = "WriteLock"
										} else {
											LockTypeName = "ReadLock"
										}
										kv.ClientInstanceSeq[cmd.ClientID] <- token
										if cmd.TimeOut > 0 {
											go func() {
												// TODO 这里其实还应该考虑数据包往返时延和双方时钟不同步的度,但是服务端大一点不影响正确性
												time.Sleep(time.Duration(cmd.TimeOut * 2) * time.Millisecond)
												// TODO 显然有条件竞争 后面改这里架构的时候再说 这里是一个难点中的难点 因为kv.mu已经成了性能瓶颈了
												// 显然这里我们需要目录的node和文件的token
												PathNode, PathOk := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
												fileNode, FileOk := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName+"/"+cmd.FileName]
												if PathOk && FileOk {
													// 全部使用最新的值保证删除成功
													PathNode.Release(fileNode.instanceSeq, cmd.FileName, fileNode.tokenSeq, fileNode.checksum)
												} else {
													log.Println("ERROR : delay delete failture.")
												}
											}()
										}
										log.Printf("INFO : [%d] Acquire file(%s) sucess, locktype is %s.\n", kv.me, cmd.FileName, LockTypeName)
										break
									} else {
										log.Printf("INFO : [%d] Acquire error (%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}
								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "Release":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									log.Printf("DEBUG : Release token is %d\n", cmd.Token)
									err := node.Release(cmd.InstanceSeq, cmd.FileName, cmd.Token, cmd.CheckSum)
									if err == nil {
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSucess // 通知机制
										// 这里只看读锁的引用计数,如果原先是读锁这就是正确的,如果是写锁Release以后也是零,是正确的;错误的话不会进入这里
										log.Printf("INFO : [%d] Release file(%s) sucess, this file reference count is %d\n", kv.me, cmd.FileName, node.readLockReferenceCount)
										break
									} else {
										log.Printf("INFO : [%d] Release error(%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue

							case "CheckToken":
								kv.ClientSeqCache[int64(cmd.ClientID)] = &LatestReply{Seq: cmd.Clientseq}
								node, ok := RootFileOperation.pathToFileSystemNodePointer[cmd.PathName]
								if ok {
									err := node.CheckToken(cmd.Token, cmd.FileName)
									if err == nil {
										kv.ClientInstanceSeq[cmd.ClientID] <- NoticeSucess // 通知机制
										// 这里只看读锁的引用计数,如果原先是读锁这就是正确的,如果是写锁Release以后也是零,是正确的;错误的话不会进入这里
										log.Printf("INFO : [%d] CheckToken file(%s) sucess, this file reference count is %d\n", kv.me, cmd.FileName, node.readLockReferenceCount)
										break
									} else {
										log.Printf("INFO : [%d] CheckToken error(%s) -> %s!\n", kv.me, cmd.PathName, err.Error())
									}
								}

								kv.ClientInstanceSeq[cmd.ClientID] <- NoticeErrorValue
							}
						} else {
							log.Println("ERROR : Multiple clients have the same ID !")
						}
					} else if cmd, ok := msg.Command.(CASOp); ok{ // CAS操作
						// log.Println("INFO : Enter CAS operation.")

						kv.mu.Lock()
						if dup, ok := kv.ClientSeqCache[int64(cmd.ClientID)]; !ok || dup.Seq < cmd.Clientseq {
							if !ok {
								kv.CASNotice[cmd.ClientID] = make(chan bool, 3)
							}
							value, err := kv.KvDictionary.ChubbyGoMapGet(cmd.Key)
							if !err { // 查询失败
								kv.CASNotice[cmd.ClientID] <- false
							} else {
								if value == cmd.Old {
									// 比较成功直接转换
									kv.KvDictionary.ChubbyGoMapSet(cmd.Key, cmd.New)
									kv.CASNotice[cmd.ClientID] <- true
								} else if cmd.Interval != 0 {
									nowValue ,flag := strconv.Atoi(value)
									if flag == nil { // 转化成功
										if cmd.boundary <= nowValue - cmd.Interval {
											kv.KvDictionary.ChubbyGoMapSet(cmd.Key, strconv.Itoa(nowValue - cmd.Interval))
											kv.CASNotice[cmd.ClientID] <- true
											//log.Printf("DEBUG : 递减目前的值 : %d.\n", nowValue - cmd.Interval)
										} else if cmd.boundary >= nowValue + cmd.Interval{
											kv.KvDictionary.ChubbyGoMapSet(cmd.Key, strconv.Itoa(nowValue + cmd.Interval))
											kv.CASNotice[cmd.ClientID] <- true
											//log.Printf("DEBUG : 递增目前的值 : %d.\n", nowValue + cmd.Interval)
										} else {
											kv.CASNotice[cmd.ClientID] <- false
										}
									} else {
										kv.CASNotice[cmd.ClientID] <- false
									}
								} else {
									// 第一遍少了这里 我服了
									kv.CASNotice[cmd.ClientID] <- false
								}
							}
						}

					}

					// msg.IsSnapshot && kv.isUpperThanMaxraftstate()
					if IsNeedSnapShot { // kv.isUpperThanMaxraftstate()
						log.Printf("INFO : [%d] need create a snapshot. maxraftstate(%d), nowRaftStateSize(%d).\n",
							kv.me, kv.maxraftstate, kv.persist.RaftStateSize())
						kv.persisterSnapshot(msg.Index) // 此index以前的数据已经打包成快照了

						/* IsSnapshot = true
						// 这个调用放在临界区内貌似比较慢,而且不需要锁的保护
						// 需要解决死锁；8月24日已解决!
						kv.rf.CreateSnapshots(msg.Index) // 使协议层进行快照*/
					}

					// 通知服务端操作
					if notifyCh, ok := kv.LogIndexNotice[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.LogIndexNotice, msg.Index)
						fmt.Printf("Notice index(%d).\n", msg.Index)
					}

					kv.mu.Unlock()

					if IsSnapShot {
						kv.rf.CreateSnapshots(msg.Index)
					}
				}
			}
		}
	}
}

/*
 * @notes: 因为这个函数中maxraftstate是不变的，RaftStateSize()又是由persist中的锁保护的，所以完全没必要放在临界区内
 */
func (kv *RaftKV) isUpperThanMaxraftstate() bool {
	if kv.maxraftstate <= 0 { // 小于等于零的时候不执行快照
		return false
	}

	NowRaftStateSize := kv.persist.RaftStateSize()

	// 后者其实存储的是Raft的状态大小，这个大小在Raft库中是在每次持久化时维护的
	if kv.maxraftstate < NowRaftStateSize {
		return true
	}
	// 以上两种是极端情况，我们需要考虑靠近临界值时就持久化快照，暂定15%
	var interval = kv.maxraftstate - NowRaftStateSize

	if interval < kv.maxraftstate/20*3 {
		return true
	}

	return false
}
