package BaseServer

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"

	ConnectError = "ConnectError"	// 最特殊的一项 用于判断对端服务器是否连接成功 如果对端还未与全部服务器连接成功 则什么也不干

	OpenError = "OpenError"
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