package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

const (
	HandleOpTimeOut = time.Millisecond * 4000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OType int

const (
	OPGet OType = iota
	OPPut
	OPAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     OType
	Key        string
	Val        string
	Seq        uint64
	Identifier int64
}

type result struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32                         // set by Kill()
	waiCh      map[int64]map[uint64]*chan Op // 2层映射 ->Identifier->Seq
	historyMap map[int64]map[uint64]*result

	maxraftstate int // snapshot if log grows this big
	db           map[string]string
}

func (kv *KVServer) DBExecute(op *Op) (res result) {
	switch op.OpType {
	case OPGet:
		val, exist := kv.db[op.Key]
		if exist {
			DPrintf("server %v DBExecute: identifier %v Seq %v:  Get(%v)= %v\n", kv.me, op.Identifier, op.Seq, op.Key, val)
			res.Value = val
			return
		} else {
			DPrintf("server %v DBExecute: identifier %v Seq %v:  Get(%v) Err:%v \n", kv.me, op.Identifier, op.Seq, op.Key, ErrKeyNotExist)
			res.Err = ErrKeyNotExist
			return
		}
	case OPPut:
		kv.db[op.Key] = op.Val
		DPrintf("server %v DBExecute: identifier %v Seq %v:  Put(%v)= %v\n", kv.me, op.Identifier, op.Seq, op.Key, op.Val)
		return
	case OPAppend:
		val, exist := kv.db[op.Key]
		if exist {
			kv.db[op.Key] = val + op.Val
			DPrintf("server %v DBExecute: identifier %v Seq %v:  OPAppend(%v,%v)= %v\n", kv.me, op.Identifier, op.Seq, op.Key, op.Val, kv.db[op.Key])
			return
		} else {
			res.Err = ErrKeyNotExist
			return
		}
	}
	return
}

func (kv *KVServer) UpdateHistory(opArgs *Op, res *result) {
	// 调用时必须持有锁
	if _, exist := kv.historyMap[opArgs.Identifier]; !exist {
		kv.historyMap[opArgs.Identifier] = make(map[uint64]*result)
	}
	kv.historyMap[opArgs.Identifier][opArgs.Seq] = res
}

func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	kv.mu.Lock()
	if _, exist1 := kv.waiCh[opArgs.Identifier]; !exist1 {
		kv.waiCh[opArgs.Identifier] = make(map[uint64]*chan Op)
	}
	if _, exist2 := kv.waiCh[opArgs.Identifier][opArgs.Seq]; !exist2 {
		// 创建一个临时通道并添加到waiCh
		ch := make(chan Op)
		kv.waiCh[opArgs.Identifier][opArgs.Seq] = &ch
	}
	ch := kv.waiCh[opArgs.Identifier][opArgs.Seq]
	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		return result{Err: ErrNotLeader, Value: ""}
	}

	// 等待消息到达或超时
	select {
	case msg, success := <-*ch:
		if !success {
			// 通道已经关闭, 有另一个协程收到了消息
			DPrintf("server %v identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息, args.OpType=%v, args.Key=%+v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key)
			res.Err = ErrChanClose
			return
		} else {
			kv.mu.Lock()
			DPrintf("server %v identifier %v Seq %v: args.OpType=%v, args.Key=%+v, 应用已提交的log到kv数据库...,", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key)
			res = kv.DBExecute(&msg)
			// 更新历史记录
			kv.UpdateHistory(opArgs, &res)
			// 关闭通道并从map中移除
			delete(kv.waiCh[opArgs.Identifier], opArgs.Seq)
			kv.mu.Unlock()
			close(*ch)
			return
		}
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		return
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// 先判断是不是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	kv.mu.Lock()
	DPrintf("leader %v 收到来自 client %v 的Get请求: Get(%v)\n", kv.me, args.Identifier, args.Key)

	// 先记录是否存在
	if clerkMap, exist1 := kv.historyMap[args.Identifier]; exist1 {
		// 客户端标识符存在, 进一步判断seq是否存在
		if rec, exist2 := clerkMap[args.Seq]; exist2 && rec != nil {
			// seq也存在, 直接返回历史记录
			kv.mu.Unlock()
			reply.Err = rec.Err
			reply.Value = rec.Value
			return
		} else {
			// 此时seq不存在, 先新建一个
			clerkMap[args.Seq] = nil
			kv.mu.Unlock()
		}
	} else {
		// 此时客户端标识符和seq都不存在, 先新建一个
		kv.historyMap[args.Identifier] = make(map[uint64]*result)
		kv.historyMap[args.Identifier][args.Seq] = nil
		kv.mu.Unlock()
	}

	opArgs := &Op{OpType: OPGet, Seq: args.Seq, Key: args.Key}
	// 执行到这里时就是没有找到历史记录的情况
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 先判断是不是leader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrNotLeader
		return
	}

	kv.mu.Lock()
	if args.Op == "Put" {
		DPrintf("leader %v 收到来自 client %v 的Put请求, Put(%v, %v)\n", kv.me, args.Identifier, args.Key, args.Value)
	} else {
		DPrintf("leader %v 收到来自 client %v 的Append请求, Append(%v, %v)\n", kv.me, args.Identifier, args.Key, args.Value)
	}

	// 先记录是否存在
	if clerkMap, exist1 := kv.historyMap[args.Identifier]; exist1 {
		// 客户端标识符存在, 进一步判断seq是否存在
		if rec, exist2 := clerkMap[args.Seq]; exist2 && rec != nil {
			// seq也存在, 直接返回历史记录
			kv.mu.Unlock()
			reply.Err = rec.Err
			return
		} else {
			// 此时seq不存在, 先新建一个
			clerkMap[args.Seq] = nil
			kv.mu.Unlock()
		}
	} else {
		// 此时客户端标识符和seq都不存在, 先新建一个
		kv.historyMap[args.Identifier] = make(map[uint64]*result)
		kv.historyMap[args.Identifier][args.Seq] = nil
		kv.mu.Unlock()
	}

	opArgs := &Op{Seq: args.Seq, Key: args.Key, Val: args.Value}

	if args.Op == "Put" {
		opArgs.OpType = OPPut
	} else {
		opArgs.OpType = OPAppend
	}

	// 执行到这里时就是没有找到历史记录的情况
	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) CommitHandler() {
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			kv.mu.Lock()
			ch, exist := kv.waiCh[op.Identifier][op.Seq]
			if !exist {
				// 接收端的通道已经被删除了, 说明这是重复的请求
				kv.mu.Unlock()
				continue
			}
			*ch <- op // TODO: 持有锁时发送是否会导致死锁?
			kv.mu.Unlock()
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.historyMap = make(map[int64]map[uint64]*result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int64]map[uint64]*chan Op)

	go kv.CommitHandler()

	return kv
}
