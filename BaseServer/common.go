package BaseServer

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"

	ConnectError = "ConnectError"	// 最特殊的一项 用于判断对端服务器是否连接成功 如果对端还未与全部服务器连接成功 则什么也不干

	OpenError = "OpenError"
	CreateError = "CreateError"
	AcquireError = "AcquireError"
	)

// 用于Delete RPC
const (
	Opdelete = iota	// 当引用计数为零时删除永久文件/目录
	Opclose	// 当引用计数为零时不删除永久文件/目录
)

type Err string

type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string
	ClientID 	uint64
	SeqNo    	int
}

type PutAppendReply struct {
	Err         Err		// 定义了六种错误 足以说明状态转换了
}

type GetArgs struct {
	Key string
	ClientID uint64
	SeqNo    int
}

type GetReply struct {
	Err         Err
	Value       string
}

type OpenArgs struct {
	ClientID uint64
	SeqNo    int
	PathName string
}

type OpenReply struct {
	Err         Err
	InstanceSeq uint64
}

/*
 * @brief: open每次结束以后如果成功会返回一个文件描述符，从而进行后面的操作，这样抽象就很符合人的使用逻辑
 */
type FileDescriptor struct {
	InstanceSeq uint64
	PathName string
}

type CreateArgs struct {
	ClientID uint64
	SeqNo    int
	InstanceSeq uint64
	FileType int
	PathName string	// 用PathName在服务器解析出节点
	FileName string	// 然后使用FileName创建文件
}

type CreateReply struct {
	Err         Err
	InstanceSeq uint64
}

type CloseArgs struct {
	ClientID uint64
	SeqNo    int
	InstanceSeq uint64
	PathName string
	FileName string
	opType	int
}

type CloseReply struct {
	Err         Err
}

type AcquireArgs struct {
	ClientID uint64
	SeqNo    int
	InstanceSeq uint64
	PathName string
	FileName string
	LockType int
}

type AcquireReply struct {
	Err         Err
	InstanceSeq uint64
}