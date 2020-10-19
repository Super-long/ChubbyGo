package KvServer

const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"
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
	Err         Err		// 定义了五种错误 足以说明状态转换了
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
