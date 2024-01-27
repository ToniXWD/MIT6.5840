package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	ErrNotLeader       = "NotLeader"
	ErrKeyNotExist     = "KeyNotExist"
	ErrHandleOpTimeOut = "HandleOpTimeOut"
	ErrChanClose       = "ChanClose"
)

// Put or Append
type PutAppendArgs struct {
	Key        string
	Value      string
	Op         string // "Put" or "Append"
	Seq        uint64
	Identifier int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key        string
	Seq        uint64
	Identifier int64
}

type GetReply struct {
	Err   Err
	Value string
}
