package shardkv

import (
	"fmt"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const DebugServer = false
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
	OK                       = "OK"
	ErrNoKey                 = "ErrNoKey"
	ErrWrongLeader           = "ErrWrongLeader"
	ErrHandleOpTimeOut       = "HandleOpTimeOut"
	ErrChanClose             = "ChanClose"
	ErrLeaderOutDated        = "LeaderOutDated"
	ErrWrongShardForCurGroup = "ErrWrongShardForCurGroup"
	ErrGroupIsInMigrant      = "ErrGroupIsInMigrant"
	ErrOldConfigForClient    = "ErrOldConfigForClient"
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
	ConfigNum  int // config number
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
