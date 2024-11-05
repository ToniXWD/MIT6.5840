package shardkv

import (
	"fmt"
	"log"
	"time"

	"6.5840/shardctrler"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const DebugServer = true
const DebugClient = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf("kv---"+format, a...)
	return
}

func ServerLog(gid int, format string, a ...interface{}) {
	if !DebugServer {
		return
	}
	server_info := fmt.Sprintf("group %v ", gid)
	DPrintf(server_info+format, a...)
}

func ClientLog(format string, a ...interface{}) {
	if !DebugClient {
		return
	}
	DPrintf("client "+format, a...)
}

const (
	// _开头的表示后续应该被移除的错误
	OK                       = "OK"
	ErrNoKey                 = "ErrNoKey"
	ErrWrongLeader           = "ErrWrongLeader"
	ErrHandleOpTimeOut       = "HandleOpTimeOut"
	ErrChanClose             = "ChanClose"
	ErrLeaderOutDated        = "LeaderOutDated"
	ErrWrongShardForCurGroup = "ErrWrongShardForCurGroup"
	ErrKVWaitForArriving     = "ErrKVWaitForArriving"
	ErrGroupIsInMigrant      = "ErrGroupIsInMigrant"
	ErrWrongConfNum          = "ErrWrongConfNum"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq        uint64
	Identifier int64
	ConfigNum  int // config number``
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq        uint64
	Identifier int64
	ConfigNum  int // config number
}

type GetReply struct {
	Err   Err
	Value string
}

type AskShardArgs struct {
	ConfigNum int // config number, 提供发起者的配置序号
	ShardIdx  []int
	SourceGid int
}

type AskShardReply struct {
	ConfigNum int // config number, 返回接受者的配置序号
	ShardDBs  map[int]*ShardDB
	Err       Err
}

// ************************************************* server.go 中定义的结构体 *************************************************
type OType string

const (
	OPGet     OType = "Get"
	OPPut     OType = "Put"
	OPAppend  OType = "Append"
	OPNewConf OType = "NewConf"
)
const (
	HandleOpTimeOut       = time.Millisecond * 2000 // 超时为 2s
	CheckNewConfigTimeOut = time.Millisecond * 2000 // 检查配置更新的超时为 2s
	RPCTimeOut            = time.Millisecond * 200  // RPC 重发的超时为 0.2s
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     OType
	Shard      int
	Key        string
	Val        string
	Seq        uint64
	Identifier int64
	ConfigNum  int
	NewConfig  *shardctrler.Config
}

type Result struct {
	LastSeq uint64
	Err     Err
	Value   string
	ResTerm int
}

// 复制 db 中的数据
func deepCopyShardDB(oldDB *ShardDB, plus bool) *ShardDB {
	newDB := &ShardDB{
		db:         make(map[string]string),
		configNum:  oldDB.configNum,
		historyMap: make(map[int64]*Result),
	}

	if plus {
		newDB.configNum++
	}

	// 复制 db 中的数据
	for k, v := range oldDB.db {
		newDB.db[k] = v
	}

	// 复制历史记录
	for id, res := range oldDB.historyMap {
		newRes := *res // 复制 Result 结构体
		newDB.historyMap[id] = &newRes
	}

	return newDB
}
