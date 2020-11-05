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

// TODO 这里有机会全部改成数字，因为每次判断的时候都需要比较，效率较差
const (
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	NoLeader   = "NoLeader"
	Duplicate  = "Duplicate"
	ReElection = "ReElection"

	ConnectError = "ConnectError"	// 最特殊的一项 用于判断对端服务器是否连接成功 如果对端还未与全部服务器连接成功 则什么也不干

	OpenError = "OpenError"
	DeleteError = "DeleteError"
	CreateError = "CreateError"
	AcquireError = "AcquireError"
	ReleaseError = "ReleaseError"
	CheckTokenError = "CheckTokenError"

	ValueCannotBeConvertedToNumber = "ValueCannotBeConvertedToNumber"
	CurrentValueExceedsExpectedValue = "CurrentValueExceedsExpectedValue"
	CASFlagUndefined = "CASFlagUndefined"
	CASFailture = "CASFailture"
	)

// 用于Delete RPC
const (
	Opdelete = iota	// 当引用计数为零时删除永久文件/目录
	Opclose	// 当引用计数为零时不删除永久文件/目录
)

const(
	NoticeErrorValue = 0
	NoticeSucess = 1
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
	ChuckSum uint64
	InstanceSeq uint64
}

/*
 * @brief: open每次结束以后如果成功会返回一个文件描述符，从而进行后面的操作，这样抽象就很符合人的使用逻辑
 * @notes: 目前全部搞成public是为了调试方便
 */
type FileDescriptor struct {
	ChuckSum uint64
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
	CheckSum uint64
}

type CloseArgs struct {
	ClientID uint64
	SeqNo    int
	InstanceSeq uint64
	PathName string
	FileName string
	OpType	int
	Checksum uint64
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
	Checksum uint64
	TimeOut	uint32	// 毫秒为单位
}

type AcquireReply struct {
	Err         Err
	Token uint64
}

type ReleaseArgs struct {
	ClientID uint64
	SeqNo    int
	InstanceSeq uint64
	PathName string
	FileName string
	Token	uint64
	CheckSum uint64
}

type ReleaseReply struct {
	Err         Err
}

type CheckTokenArgs struct {
	ClientID uint64
	SeqNo    int
	Token uint64
	PathName string
	FileName string
}

type CheckTokenReply struct {
	Err         Err
}

type CompareAndSwapArgs struct{
	ClientID 	uint64
	SeqNo    	int
	Key			string
	Old			int
	New  		int
	Flag 		int
}

type CompareAndSwapReply struct {
	Err         Err
}

/*
 * @brief: 对不齐就很烦,go Ctrl+Alt+L 会自动把注释后推;考虑到可能使用url作为文件名
 */
var isUrlEffective = [128]bool{
	/*0   nul    soh    stx    etx    eot    enq    ack    bel     7*/
	false, false, false, false, false, false, false, false,
	/*8   bs     ht     nl     vt     np     cr     so     si     15*/
	false, false, false, false, false, false, false, false,
	/*16  dle    dc1    dc2    dc3    dc4    nak    syn    etb    23*/
	false, false, false, false, false, false, false, false,
	/*24  can    em     sub    esc    fs     gs     rs     us     31*/
	false, false, false, false, false, false, false, false,
	/*32  ' '    !      "      #     $     %     &     '          39*/
	false, false, false, true, true, true, true, false,
	/*40  (      )      *      +     ,     -     .     /          47*/
	false, false, false, true, true, true, true, true,
	/*48  0     1     2     3     4     5     6     7             55*/
	true, true, true, true, true, true, true, true,
	/*56  8     9     :     ;     <      =     >      ?           63*/
	true, true, true, true, false, true, false, true,
	/*64  @     A     B     C     D     E     F     G             71*/
	true, true, true, true, true, true, true, true,
	/*72  H     I     J     K     L     M     N     O             79*/
	true, true, true, true, true, true, true, true,
	/*80  P     Q     R     S     T     U     V     W             87*/
	true, true, true, true, true, true, true, true,
	/*88  X     Y     Z     [      \      ]      ^      _         95*/
	true, true, true, false, false, false, false, true,
	/*96  `      a     b     c     d     e     f     g           103*/
	false, true, true, true, true, true, true, true,
	/*104 h     i     j     k     l     m     n     o            113*/
	true, true, true, true, true, true, true, true,
	/*112 p     q     r     s     t     u     v     w            119*/
	true, true, true, true, true, true, true, true,
	/*120 x     y     z     {      |      }      ~      del      127*/
	true, true, true, false, false, false, false, false,
}

/*
 * @brief: 判断文件名是否出现错误
 */
func isEffective(ch byte) bool {
	if ch < 0 { //|| ch > 127{ 	显然不太可能
		return false
	} else {
		return isUrlEffective[ch]
	}
}