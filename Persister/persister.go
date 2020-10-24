package Persister

import (
	"io/ioutil"
	"log"
	"os"
	"sync"
)

// 模仿redis AOF
const(
	Always = iota	// 每条操作都进行刷盘 TODO 目前并不太好实现
	Everysec		// 每秒进行一次刷盘,把raftstate和snapshot分别存到不同的文件中
	No				// 不主动刷盘,也就隐含着我们不需要启动守护协程
)

type Persister struct {
	mu        sync.Mutex	// 这个锁很重要,因为raft会和守护协程都会操作这个结构体,所以需要加锁保护
	raftstate []byte
	snapshot  []byte

	// 持久化文件名取决于用户给予了用户很高的自由度,同时方便了测试
	SnapshotFileName	string	// 快照的持久化文件名
	RaftstateFileName	string	// raft状态的持久化名
	PersistenceStrategy int		// 对应着三条策略
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = state
	ps.snapshot = snapshot
}

func (ps *Persister) SaveSnapshot(snapshot []byte){
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.snapshot = snapshot
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.snapshot
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// 考虑到要把快照和基础配置放在两个文件中,这里不把下面两个函数搞成成员函数,这样可以减少代码重复

/*
 * @brief: 向文件写入数据
 * @return: 返回错误类型
 * @notes: 暂定给出存盘策略,遵循所选择策略刷入磁盘,一个守护协程负责按照策略刷入磁盘
 */
// TODO 目前需要json中选择持久化策略和存储的文件名,
//  需要在raft开始时跑一个守护协程,负责周期刷盘
//  并在服务器解析出文件名的时候进行读取,把数据传给persist,persist传给Raft
func WriteContentToFile(vals []byte, outfile string) error {
	file, err := os.Create(outfile)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	number ,errW := file.Write(vals)
	if errW != nil || number < len(vals){
		log.Println(errW.Error())
		return errW
	}

	errC := file.Close()
	if errC != nil{
		log.Println(errC.Error())
		return errC
	}
	// TODO close失败时调用方行为怎么做？
	return nil
}

/*
 * @brief: 从文件中读出数据
 * @return: 返回文件的[]byte和错误类型
 * @notes: 使用这个函数的时候必须先判断err,再使用[]byte,被调用于服务器启动时,先读取两个文件,放到persister,然后传递给Raft,启动时就会自动读取了
 */
func ReadContentFromFile(filepath string) ([]byte, error) {
	var NULL []byte
	//打开文件
	fi, err := os.Open(filepath)
	if err != nil{
		log.Println(err.Error())
		return NULL ,err
	}

	//读取内容
	res, errR := ioutil.ReadAll(fi)
	if err != nil{
		log.Println(errR.Error())
		return res, errR
	}

	errC := fi.Close()
	if errC != nil{
		log.Println(errC.Error())
		return res, errC
	}
	// 没有错误，直接退出
	return res, nil
}