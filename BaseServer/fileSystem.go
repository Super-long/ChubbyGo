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
	"crypto/sha1"
	"fmt"
	"log"
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

type ChubbyGoFileSystemError int8

const (
	PathError = iota
	CheckSumError
	InstanceSeqError
	DoesNotSupportRecursiveDeletion
	CannotDeleteFilesWithZeroReferenceCount
	LockTypeError
	ReleaseBeforeAcquire
	OnlyDirectoriesCanCreateFiles
	TokenSeqError
	FileNameError
)

func (err ChubbyGoFileSystemError) Error() string{
	var ans string
	switch err {
	case PathError:
		ans = "Filename and directory have some name."
		break
	case CheckSumError:
		ans = "Client send a error CheckSum."
		break
	case InstanceSeqError:
		ans = "Client send a error InstanceSeq."
		break
	case DoesNotSupportRecursiveDeletion:
		ans = "Does not support recursive deletion, there are still files in this directory."
		break
	case CannotDeleteFilesWithZeroReferenceCount:
		ans = "Deleting a file with a reference count of zero."
		break
	case LockTypeError:
		ans = "Lock type error."
		break
	case ReleaseBeforeAcquire:
		ans = "The lock being released has not been locked."
		break
	case OnlyDirectoriesCanCreateFiles:
		ans = "Only directories can create files."
		break
	case TokenSeqError:
		ans = "Client send a error TokenSeq."
		break
	case FileNameError:
		ans = "The inserted file has disallowed characters."
		break
	default:

	}
	return ans
}

type FileSystemNode struct {
	// 在此文件下创建新文件时要传入的参数
	fileType   int       // 三种文件类型; 不支持链接文件
	readACLs   *[]uint64 // 读权限access control lists
	writeACLs  *[]uint64 // 写权限,请求任意类型的锁都需要写权限
	modifyACLs *[]uint64 // 修改此节点ACL的权限列表
	fileName   string    // 此文件的名称

	instanceSeq uint64 // 实例编号:大于任意先前的同名节点的实例编号
	tokenSeq    uint64 // 锁生成编号:当节点的锁从空闲状态转换到被持有状态时，这个值会增加。用于在持有锁超时时取消上一个seq的权限
	aCLsSeq     uint64 // ACL生成编号：当写入节点的 ACL 名称时，这种情况会增加。	// 目前没有看懂有什么用处

	// 本来想加入FileSystemNode地址,但是go的栈会变,所以打消了这个念头
	// 因为在文件描述符期间instanceSeq不会改变,但总体是改变的;
	// 如此看来Checksum不一定是整个结构体的某种校验和,只要保证随机性就ok了;这句话想的大错特错,因为每个节点会受到相同的checksum,如果多个节点生成的不一样会导致leader成功,其他节点失败
	checksum uint64 // 返回客户端用以构造一个难以被伪造的文件描述符

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
 * @!!!!!: 完全想错了;我们需要一个以文件内容生成的校验和,这样才可以在不同的节点都执行成功
 */
/*func (Fsn *FileSystemNode) makeCheckSum() uint64 {
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
	rand.Seed(time.Now().Unix()) // 用时间作为随机数种子
	high := rand.Intn(2<<22 - 1) // 伪随机数

	res += uint64(high) << 42

	// [63  42][41  32][31  0]
	// [随机数][instanceSeq][address]
	return res
}*/

/*
 * @notes: 用instanceSeq,nowPath,len(next),len(nextNameCache)为参数生成64位校验和,完全没有必要使用CRC,MD5,
 * 只要保证相同的输入能得到相同的输出就可以了; 这样可能会多次open文件的checksum相同
 * 显然instanceSeq最小值为1，checksum也不可能是0，所以ClientInstanceSeq和ClientInstanceSeq可以使用0作为无效值
 */
func (Fsn *FileSystemNode) makeCheckSum() uint64 {
	// 生成20位字符串,每一位都是16进制的数字
	hash := sha1.New()
	hash.Write([]byte(Fsn.nowPath))
	sha1Res := hash.Sum(nil)

	var res uint64 = 0

	for i := 0; i < 14; i++ {
		res = res | (uint64(sha1Res[i]) << (4*i))
	}

	//[63,56][55,0]

	res = res | Fsn.instanceSeq << 57

	res = res | uint64(len(Fsn.next) << 59)

	res = res | uint64(len(Fsn.nextNameCache) << 61)

	return res
}

/*
 * @brief: Type:创建的文件的类型;name:文件名称;后面则是文件初始的操作权限
 * @return: 插入正确返回true;否则返回false
 * @notes: 文件被创建的时候默认打开,即引用计数为1,能够插入文件证明客户端的InstanceSeq是目录的
 */
func (Fsn *FileSystemNode) Insert(InstanceSeq uint64, Type int, name string, ReadAcl *[]uint64, WriteAcl *[]uint64, ModifyAcl *[]uint64) (uint64, uint64, error) {
	if InstanceSeq < Fsn.instanceSeq {
		return 0, 0, ChubbyGoFileSystemError(InstanceSeqError)
	}

	if Fsn.fileType != Directory { // 仅目录允许创建新文件
		return 0, 0, ChubbyGoFileSystemError(OnlyDirectoriesCanCreateFiles)
	}

	// 文件名只要不包含'/'和' '就可以了，后面加上url判断以后需要修改这里
	bytesFileName := str2sbyte(name)
	bytesLength := len(bytesFileName)
	for i:=0; i<bytesLength;i++{
		if bytesFileName[i] == '/' || bytesFileName[i] == ' '{
			return 0, 0, ChubbyGoFileSystemError(FileNameError)
		}
	}

	// 目录与文件不可以重名
	_, IsExist := Fsn.next[name]
	if IsExist { // Fsn中存在着与这个文件文件名相同的文件
		return 0, 0, ChubbyGoFileSystemError(PathError)
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

	// 初始值设定为2，协定0为无效值.1为delete的删除值
	if Seq, ok:=Fsn.nextNameCache[name]; ok{
		NewNode.instanceSeq = Seq // 使用的时候直接用就好，delete的时候会递增
	} else {
		NewNode.instanceSeq = 2
		Fsn.nextNameCache[name] = 2
	}
	NewNode.tokenSeq = 2      // 使用的时候先递增，再取值，也就是说TockenSeq最低有效值是2
	NewNode.aCLsSeq = 0
	NewNode.checksum = NewNode.makeCheckSum()

	NewNode.nowLockType = NotLock
	NewNode.readLockReferenceCount = 0
	// 需要在每一个Cell中保证唯一性
	NewNode.nowPath = Fsn.nowPath + "/" + name
	NewNode.OpenReferenceCount = 1
	Fsn.next[name] = NewNode

	RootFileOperation.pathToFileSystemNodePointer[NewNode.nowPath] = NewNode

	return NewNode.instanceSeq, NewNode.checksum, nil
}

/*
 * @param: 文件描述符传来的InstanceSeq，要删除的文件的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在;
 */
func (Fsn *FileSystemNode) Delete(InstanceSeq uint64, filename string, opType int, checkSum uint64) error {
	Node, IsExist := Fsn.next[filename]
	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return ChubbyGoFileSystemError(PathError)
	}

	if checkSum != Node.checksum {
		log.Printf("WARNING : A danger requirment, unmatched checksum, now(%d) -> client(%d).\n", Node.checksum, checkSum)
		return ChubbyGoFileSystemError(CheckSumError)
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Delete -> Request from a backward file descriptor!")
		return ChubbyGoFileSystemError(InstanceSeqError)
	}

	if Node.fileType == Directory && len(Fsn.next) != 0 {
		log.Println("WARNING : Recursive deletion of files is currently not allowed!")
		return ChubbyGoFileSystemError(DoesNotSupportRecursiveDeletion) // TODO 目前不支持这种递归删除，因为下面文件可能还持有锁，这个后面再说
	}

	if Node.OpenReferenceCount <= 0 {
		log.Printf("ERROR : Delete a file(%s/%s) that referenceCount is zero.\n", Fsn.nowPath, filename)
		return ChubbyGoFileSystemError(CannotDeleteFilesWithZeroReferenceCount)
	}

	Node.OpenReferenceCount--

	// close :引用计数为零时只有临时文件会被删除，永久文件和目录文件都不会被删除
	// delete: 相反
	if Node.OpenReferenceCount != 0 {
		return nil // delete成功，其实只是把客户端的句柄消除掉而已
	}

	// name存在,引用计数为零且与远端InstanceSeq相等，可以执行删除
	if opType == Opdelete || Node.fileType == TemporaryFile { // 此次是delete操作,如果是close操作的话永久文件和目录则不需要删除
		delete(Fsn.next, filename)
		Fsn.nextNameCache[filename]++ // 下一次创建的时候INstanceSeq与上一次不同
	}

	// 当文件的引用计数为零的时候更新Checksum,也就说下一次打开时得到的句柄是不一样的,可以有效防止客户端伪造checkSum
	Node.checksum = Node.makeCheckSum()

	return nil
}

/*
 * @param: 文件描述符传来的InstanceSeq;要删除的文件的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在; 调用方先判断bool再看seq
 */
func (Fsn *FileSystemNode) Acquire(InstanceSeq uint64, filename string, locktype int, checksum uint64) (uint64, error) {
	Node, IsExist := Fsn.next[filename]

	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return 0, ChubbyGoFileSystemError(PathError)
	}

	if checksum != Node.checksum {
		log.Printf("WARNING : A danger requirment, unmatched checksum, now(%d) -> client(%d).\n", checksum, Node.checksum)
		return 0, ChubbyGoFileSystemError(CheckSumError)
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Acquire -> Request from a backward file descriptor!")
		return 0, ChubbyGoFileSystemError(InstanceSeqError)
	}

	if Node.nowLockType == NotLock {
		if locktype == ReadLock {
			Node.readLockReferenceCount++
		}
		Node.nowLockType = locktype
	} else if Node.nowLockType == ReadLock && locktype == ReadLock {
		Node.readLockReferenceCount++
		return Node.tokenSeq, nil
	} else if Node.nowLockType == WriteLock { // 和下面分开写是为了清楚的看到所有情况
		return 0, ChubbyGoFileSystemError(LockTypeError)
	} else { // now readLock, args writelock
		return 0, ChubbyGoFileSystemError(LockTypeError)
	}

	return Node.tokenSeq, nil // 直接返回当前值，在release的时候加1就可以了
}

/*
 * @param: 文件描述符传来的InstanceSeq;要释放的文件(锁)的名字
 * @return: 成功删除返回true，否则返回false
 * @notes: 需要检测name是否存在
 */
func (Fsn *FileSystemNode) Release(InstanceSeq uint64, filename string, Token uint64, checksum uint64) error {
	Node, IsExist := Fsn.next[filename]

	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return ChubbyGoFileSystemError(PathError)
	}

	if checksum != Node.checksum {
		log.Printf("WARNING : A danger requirment, unmatched checksum, now(%d) -> client(%d).\n", checksum, Node.checksum)
		return ChubbyGoFileSystemError(CheckSumError)
	}

	// 防止落后的锁持有者删除掉现有的被其他节点持有的锁
	if Token < Node.tokenSeq {
		log.Printf("WARNING : Have a lagging client want release file(%s) now(%d) node.clientSeq(%d).\n", Node.nowPath, Token, Node.tokenSeq)
		return ChubbyGoFileSystemError(TokenSeqError)
	}

	if InstanceSeq < Node.instanceSeq {
		log.Println("WARNING : Release -> Request from a backward file descriptor!")
		return ChubbyGoFileSystemError(InstanceSeqError)
	}

	if Node.nowLockType == NotLock {
		log.Println("WARNING : Error operation, release before acquire.")
		return ChubbyGoFileSystemError(ReleaseBeforeAcquire)
	} else if Node.nowLockType == ReadLock { // TODO 但显然这种做法会使得写操作可能饥饿
		if Node.readLockReferenceCount >= 1 {
			Node.readLockReferenceCount--
		} else { // 显然不可能出现这种情况
			log.Println("ERROR : Release -> Impossible situation.")
		}
	}

	if Node.readLockReferenceCount > 0 {
		return nil
	}

	// 当前锁定类型是写锁或者读锁引用计数为0,显然当所有的读锁都解锁以后才会递增token
	Node.tokenSeq++
	Node.nowLockType = NotLock

	return nil
}

/*
 * @param: 文件描述符传来的InstanceSeq;要打开的文件名
 * @return: 返回当前文件的instanceSeq
 * @notes: 对于一个文件来说客户端open操作可以检查某个文件是否存在,如果存在会返回一个句柄,反之返回false;
		对于一个目录来说open可以使其获取句柄后创建文件;
*/
func (Fsn *FileSystemNode) Open(name string) (uint64, uint64) {
	// TODO 这里应该做一些权限的检测，但是现在并没有想好如何划分权限

	// Open返回的应该是本文件的instanceSeq
	return Fsn.instanceSeq, Fsn.checksum
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

	// instanceseq与tokenseq的初始值是2，零设定为无效值,1为delete通知的成功值
	root.instanceSeq = 2
	root.tokenSeq = 2
	root.aCLsSeq = 0
	root.checksum = root.makeCheckSum()

	root.nowLockType = NotLock
	root.readLockReferenceCount = 0
	root.nowPath = "/ls/" + root.fileName // 所有Cell的根均为ls(lock server),这是一个虚拟的根
	root.next = make(map[string]*FileSystemNode)
	root.nextNameCache = make(map[string]uint64)

	/*	log.Printf("目前的路径名 : %s\n", root.nowPath)
		RootFileOperation.pathToFileSystemNodePointer[root.nowPath] = root*/

	return root
}

func (Fsn *FileSystemNode) CheckToken(token uint64, filename string) error{
	Node, IsExist := Fsn.next[filename]

	if !IsExist {
		log.Printf("INFO : %s/%s does not exist.\n", Fsn.nowPath, filename)
		return ChubbyGoFileSystemError(PathError)
	}

	if Node.tokenSeq == token{
		return nil
	} else {
		return ChubbyGoFileSystemError(TokenSeqError)
	}
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
