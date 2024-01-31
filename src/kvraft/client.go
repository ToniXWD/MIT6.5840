package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const (
	RpcRetryInterval = time.Millisecond * 50
)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	seq        uint64
	identifier int64
	leaderId   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GetSeq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.identifier = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, Seq: ck.GetSeq(), Identifier: ck.identifier}

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated {
			if !ok {
				reply.Err = ERRRPCFailed
			}
			if reply.Err != ErrNotLeader {
				DPrintf("clerk %v Seq %v 重试Get(%v), Err=%s", args.Identifier, args.Key, args.Key, reply.Err)
			}

			ck.leaderId += 1
			ck.leaderId %= len(ck.servers)
			time.Sleep(RpcRetryInterval)
			continue
		}

		switch reply.Err {
		case ErrChanClose:
			DPrintf("clerk %v Seq %v 重试Get(%v), Err=%s", args.Identifier, args.Key, args.Key, reply.Err)
			time.Sleep(time.Microsecond * 5)
			continue
		case ErrHandleOpTimeOut:
			DPrintf("clerk %v Seq %v 重试Get(%v), Err=%s", args.Identifier, args.Key, args.Key, reply.Err)
			time.Sleep(RpcRetryInterval)
			continue
		case ErrKeyNotExist:
			DPrintf("clerk %v Seq %v 成功: Get(%v)=%v, Err=%s", args.Identifier, args.Key, args.Key, reply.Value, reply.Err)
			return reply.Value
		}
		DPrintf("clerk %v Seq %v 成功: Get(%v)=%v, Err=%s", args.Identifier, args.Key, args.Key, reply.Value, reply.Err)

		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.GetSeq(), Identifier: ck.identifier}

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", args, reply)
		if !ok || reply.Err == ErrNotLeader || reply.Err == ErrLeaderOutDated {
			if !ok {
				reply.Err = ERRRPCFailed
			}
			if reply.Err != ErrNotLeader {
				DPrintf("clerk %v Seq %v 重试%s(%v, %v), Err=%s", args.Identifier, args.Key, args.Op, args.Key, args.Value, reply.Err)
			}

			ck.leaderId += 1
			ck.leaderId %= len(ck.servers)
			time.Sleep(RpcRetryInterval)
			continue
		}

		switch reply.Err {
		case ErrChanClose:
			DPrintf("clerk %v Seq %v 重试%s(%v, %v), Err=%s", args.Identifier, args.Key, args.Op, args.Key, args.Value, reply.Err)
			time.Sleep(RpcRetryInterval)
			continue
		case ErrHandleOpTimeOut:
			DPrintf("clerk %v Seq %v 重试%s(%v, %v), Err=%s", args.Identifier, args.Key, args.Op, args.Key, args.Value, reply.Err)
			time.Sleep(RpcRetryInterval)
			continue
		}
		DPrintf("clerk %v Seq %v 成功: %s(%v, %v), Err=%s", args.Identifier, args.Key, args.Op, args.Key, args.Value, reply.Err)

		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
