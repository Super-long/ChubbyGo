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
	"math/rand"
	"reflect"
	"time"
	"unsafe"
)

// ps:文件系统中的所有操作都建立在有锁的基础上

const (
	Directory     = iota
	TemporaryFile // 瞬时节点会在没有客户端打开它时自动删除
	PermanentFile
)

const (
	NotLock = iota
	ReadLock
	WriteLock
)

type FileSystemNode struct {
	// 在此文件下创建新文件时要传入的参数
	fileType   int       // 三种文件类型; 不支持链接文件
	readACLs   *[]uint64 // 读权限access control lists
	writeACLs  *[]uint64 // 写权限,请求任意类型的锁都需要写权限
	modifyACLs *[]uint64 // 修改此节点ACL的权限列表
	fileName   string    // 此文件的名称

	instanceSeq uint64	// 实例编号:大于任意先前的同名节点的实例编号
	tokenSeq   uint64	// 锁生成编号:当节点的锁从空闲状态转换到被持有状态时，这个值会增加。用于在持有锁超时时取消上一个seq的权限
	aCLsSeq     uint64	// ACL生成编号：当写入节点的 ACL 名称时，这种情况会增加。	// 目前没有看懂有什么用处

	// 本来想加入FileSystemNode地址,但是go的栈会变,所以打消了这个念头
	// 因为在文件描述符期间instanceSeq不会改变,但总体是改变的;
	// 如此看来Checksum不一定是整个结构体的某种校验和,只要保证随机性就ok了
	// TODO 当文件的引用计数delete为零时修改Checksum,其他时候不管是open返回还是检查直接操作节点内的Checksum就ok;
	Checksum	uint64	// 返回客户端用以构造一个难以被伪造的文件描述符

	OpenReferenceCount     uint64 // open的引用计数,主要用于文件夹,可以理解为文件的描述符没什么意义,文件有意义的是LockType
	nowLockType            int    // 目前的上锁类型,有三种情况
	readLockReferenceCount uint64 // 读锁的引用计数

	// 提供的锁是建议性锁，也就是不加锁也可以通过get，put操作对于文件内容进行操作，不过只提供string的存储，客户可以自定义协议
	nowPath       string                     // 标记当前的路径，用于从raft日志字典中获取此文件中存储的值
	nextNameCache map[string]uint64          // 其实就是下一级的InstanceSeq;当该节点是文件时此字段无效
	next          map[string]*FileSystemNode // 使用map可以更快的找到下一级的全部文件节点，且有序;当该节点是文件时此字段无效
}

/*
 * @brief: 基于FileSystemNode生成一个新的CheckSum; 其实保证随机性就ok了;我这种生成方式最大程度与文件有关
 * @notes: 大费周章的生成这样一个数是否有必要呢?虽然用的也不多
 */
func (Fsn *FileSystemNode) makeCheckSum() uint64 {
	var res uint64

	// step1: 取对象地址的值
	// AddressValue可以获取对象的地址的64位整数具体的值
	p := reflect.ValueOf(unsafe.Pointer(&Fsn))
	// 因为整个地址的低位是0Xc起头的，有一位可以推出来，程序比较小的时候很多位都可以推出来，所以我们使用右边32位
	// var AddressValue uint64 = uint64(p.Pointer())
	res = uint64(p.Pointer())

	// step2: 得instanceSeq的值，暂定后十位
	res += Fsn.instanceSeq << 32

	// step3: 随机生成一个值
	rand.Seed(time.Now().Unix())	// 用时间作为随机数种子
	high := rand.Intn(2<<22 - 1)	// 伪随机数

	res += uint64(high) << 42

	// [63  42][41  32][31  0]
	// [随机数][instanceSeq][address]
	return res
}

/*
 * @brief: Type:创建的文件的类型;name:文件名称;后面则是文件初始的操作权限
 * @return: 插入正确返回true;否则返回false
 * @notes: 文件被创建的时候默认打开,即引用计数为1,能够插入文件证明客户端的InstanceSeq是目录的
 */
func (Fsn *FileSystemNode) Insert(InstanceSeq uint64, Type int, name string, ReadAcl *[]uint64, WriteAcl *[]uint64, ModifyAcl *[]uint64) (uint64, bool) {
	if InstanceSeq < Fsn.instanceSeq {
		return 0, false
	}

	if Fsn.fileType != Directory { // 仅目录允许创建新文件
		return 0, false
	}

	// TODO 忘记加文件名称解析了，可能被字符串攻击
	// 目录与文件不可以重名
	_, IsExist := Fsn.next[name]
	if IsExist { // Fsn中存在着与这个文件文件名相同的文件
		return 0, false
	}

	NewNode := &FileSystemNode{}

	NewNode.fileType = Type
	if Type == Directory {
		NewNode.next = make(map[string]*FileSystemNode)
		NewNode.nextNameCache = make(map[string]uint64)
	}

	NewNode.fileName = name
	NewNode.readACLs = ReadAcl
	NewNode.writeACLs = WriteAcl
	NewNode.modifyACLs = ModifyAcl

	Seq := Fsn.nextNameCache[name]
	NewNode.instanceSeq = Seq // 使用的时候直接用就好，delete的时候会递增
	NewNode.tokenSeq = 0     // 使用的时候先递增，再取值，也就是说TockenSeq最低有效值是1
	NewNode.aCLsSeq = 0

	NewNode.nowLockType = NotLock
	NewNode.readLockReferenceCount = 0
	// 需要在每一个Cell中保证唯一性
	NewNode.nowPath = Fsn.nowPath + "/" + name
	NewNode.OpenReferenceCount = 1
	Fsn.next[name] = NewNode

	RootFileOperation.pathToFileSystemNodePointer[NewNode.nowPath] = NewNode

	return NewNode.instanceSeq, true
}

/*
 * @param: 文件描述符传来的InstanceSeq，要删除的文件的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在;
 */
func (Fsn *FileSystemNode) Delete(InstanceSeq uint64, filename string, opType int) bool {
	Node, IsExist := Fsn.next[filename]
	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return false
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Delete -> Request from a backward file descriptor!")
		return false
	}

	if Node.fileType == Directory && len(Fsn.next) != 0 {
		log.Println("WARNING : Recursive deletion of files is currently not allowed!")
		return false // TODO 目前不支持这种递归删除，因为下面文件可能还持有锁，这个后面再说
	}

	Node.OpenReferenceCount--

	// close :引用计数为零时只有临时文件会被删除，永久文件和目录文件都不会被删除
	// delete: 相反
	if Node.OpenReferenceCount != 0 {
		return true // delete成功，其实只是把客户端的句柄消除掉而已
	}

	// name存在,引用计数为零且与远端InstanceSeq相等，可以执行删除
	if opType == Opdelete || Node.fileType == TemporaryFile { // 此次是delete操作,如果是close操作的话永久文件和目录则不需要删除
		delete(Fsn.next, filename)
		Fsn.nextNameCache[filename]++ // 下一次创建的时候INstanceSeq与上一次不同
	}

	return true
}

/*
 * @param: 文件描述符传来的InstanceSeq;要删除的文件的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在; 调用方先判断bool再看seq
 */
func (Fsn *FileSystemNode) Acquire(InstanceSeq uint64, filename string, LockType int) (uint64, bool) {
	Node, IsExist := Fsn.next[filename]

	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return 0, false
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Acquire -> Request from a backward file descriptor!")
		return 0, false
	}

	if Node.nowLockType == NotLock {
		if LockType == ReadLock {
			Node.readLockReferenceCount++
		}
		Node.nowLockType = LockType
	} else if Node.nowLockType == ReadLock && LockType == ReadLock {
		Node.readLockReferenceCount++
		return Node.tokenSeq, true
	} else if Node.nowLockType == WriteLock { // 和下面分开写是为了清楚的看到所有情况
		return 0, false
	} else { // now readLock, args writelock
		return 0, false
	}

	return Node.tokenSeq, true // 直接返回当前值，在release的时候加1就可以了
}

/*
 * @param: 文件描述符传来的InstanceSeq;要释放的文件(锁)的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在
 */
func (Fsn *FileSystemNode) Release(InstanceSeq uint64, filename string, Tocken uint64) bool {
	Node, IsExist := Fsn.next[filename]

	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return false
	}

	// 防止落后的锁持有者删除掉现有的被其他节点持有的锁
	if Tocken < Node.tokenSeq {
		log.Printf("WARNING : Have a lagging client want release file(%s).\n", Node.nowPath)
		return false
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Release -> Request from a backward file descriptor!")
		return false
	}

	if Node.nowLockType == NotLock {
		log.Println("WARNING : Error operation, release before acquire.")
		return false
	} else if Node.nowLockType == ReadLock { // TODO 但显然这种做法会使得写操作可能饥饿
		if Node.readLockReferenceCount >= 1 {
			Node.readLockReferenceCount--
		} else { // 显然不可能出现这种情况
			log.Println("ERROR : Release -> Impossible situation.")
		}
	}

	if Node.readLockReferenceCount > 0 {
		return true
	}

	// 当前锁定类型是写锁或者读锁引用计数为0,显然当所有的读锁都解锁以后才会递增token
	Node.tokenSeq++
	Node.nowLockType = NotLock

	return true
}

/*
 * @param: 文件描述符传来的InstanceSeq;要打开的文件名
 * @return: 返回当前文件的instanceSeq
 * @notes: 对于一个文件来说客户端open操作可以检查某个文件是否存在,如果存在会返回一个句柄,反之返回false;
		对于一个目录来说open可以使其获取句柄后创建文件;
*/
func (Fsn *FileSystemNode) Open(name string) uint64 {
	// TODO 这里应该做一些权限的检测，但是现在并没有想好如何划分权限，先不急，返回当前InstanceSeq

	// Open返回的应该是本文件的instanceSeq
	return Fsn.instanceSeq
}

/*
 * @brief: 用于初始化每个Cell内的根目录
 * @return: 返回根目录的实体
 */
func InitRoot() *FileSystemNode {
	root := &FileSystemNode{}

	root.fileType = Directory
	// TODO 访问控制如何调整 就是一系列的ACLs 这个显然需要读取配置文件了

	// 想来想去，如果把Chubby Cell的名字改成与leaderID，后面切主会比较麻烦，对于外界来说也不好调用
	// 因为理想的调用方法是有一个DNS服务器，客户端可以使用名称来得到这个Cell的地址
	// 此时直接取名就方便的多，TODO 后面这里还是要读一手配置文件
	//root.fileName = "ChubbyCell_" + strconv.Itoa(int(leaderID))
	root.fileName = "ChubbyCell_" + "lizhaolong"

	root.instanceSeq = 0
	root.tokenSeq = 0
	root.aCLsSeq = 0

	root.nowLockType = NotLock
	root.readLockReferenceCount = 0
	root.nowPath = "/ls/" + root.fileName // 所有Cell的根均为ls(lock server),这是一个虚拟的根
	root.next = make(map[string]*FileSystemNode)
	root.nextNameCache = make(map[string]uint64)

	/*	log.Printf("目前的路径名 : %s\n", root.nowPath)
		RootFileOperation.pathToFileSystemNodePointer[root.nowPath] = root*/

	return root
}

/*
 * @brief: Debug用,显示当前目录树的全部文件名
 */
func RecursionDisplay(Fsn *FileSystemNode) {
	fmt.Println(Fsn.nowPath)
	for _, value := range Fsn.next {
		RecursionDisplay(value)
	}
}

/*
 * @brief: Debug用,输出间隔符号
 */
func IntervalDisPlay() {
	fmt.Println("----------------------------------")
}
