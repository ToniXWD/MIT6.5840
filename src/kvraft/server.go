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
	HandleOpTimeOut = time.Millisecond * 100
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
	LastSeq uint64
	Err     Err
	Value   string
}

type KVServer struct {
	mu         sync.Mutex
	me         int
	rf         *raft.Raft
	applyCh    chan raft.ApplyMsg
	dead       int32                  // set by Kill()
	waiCh      map[int64]*chan result // 映射 Identifier->ch
	historyMap map[int64]*result      // 映射 Identifier->*result

	maxraftstate int // snapshot if log grows this big
	maxMapLen    int
	db           map[string]string
}

func (kv *KVServer) LogInfoReceive(opArgs *Op, logType int) {
	// logType:
	// 	0: 新的请求
	// 	1: 重复的请求
	// 	2: 旧的请求
	needPanic := false
	dateStr := ""
	if logType == 0 {
		dateStr = "新的"
	} else if logType == 1 {
		dateStr = "重复"
	} else {
		dateStr = "旧的"
		needPanic = true
	}
	switch opArgs.OpType {
	case OPGet:
		DPrintf("leader %v identifier %v Seq %v %sGet请求: Get(%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key)
	case OPPut:
		DPrintf("leader %v identifier %v Seq %v %sPut请求: Put(%v,%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key, opArgs.Val)
	case OPAppend:
		DPrintf("leader %v identifier %v Seq %v %sPut请求: Put(%v,%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key, opArgs.Val)
	}

	if needPanic {
		panic("没有记录更早的请求的结果")
	}
}

func (kv *KVServer) LogInfoDBExecute(opArgs *Op, err Err, res string, isLeader bool) {
	role := "follower"
	if isLeader {
		role = "leader"
	}
	switch opArgs.OpType {
	case OPGet:
		if err != "" {
			DPrintf("%s %v identifier %v Seq %v DB执行Get请求: Get(%v), Err=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, err)
		} else {
			DPrintf("%s %v identifier %v Seq %v DB执行Get请求: Get(%v), res=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, res)
		}
	case OPPut:
		if err != "" {
			DPrintf("%s %v identifier %v Seq %v DB执行Put请求: Put(%v,%v), Err=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, err)

		} else {
			DPrintf("%s %v identifier %v Seq %v DB执行Put请求: Put(%v,%v), res=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, res)
		}
	case OPAppend:
		if err != "" {
			DPrintf("%s %v identifier %v Seq %v DB执行Append请求: Put(%v,%v), Err=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, err)
		} else {
			DPrintf("%s %v identifier %v Seq %v DB执行Append请求: Put(%v,%v), res=%s\n", role, kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, res)
		}
	}
}

func (kv *KVServer) DBExecute(op *Op, isLeader bool) (res result) {
	// 调用该函数需要持有锁
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPGet:
		val, exist := kv.db[op.Key]
		if exist {
			kv.LogInfoDBExecute(op, "", val, isLeader)
			res.Value = val
			return
		} else {
			res.Err = ErrKeyNotExist
			kv.LogInfoDBExecute(op, "", ErrKeyNotExist, isLeader)
			return
		}
	case OPPut:
		kv.db[op.Key] = op.Val
		kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
		return
	case OPAppend:
		val, exist := kv.db[op.Key]
		if exist {
			kv.db[op.Key] = val + op.Val
			kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
			return
		} else {
			kv.db[op.Key] = op.Val
			kv.LogInfoDBExecute(op, "", kv.db[op.Key], isLeader)
			return
		}
	}
	return
}

func (kv *KVServer) HandleOp(opArgs *Op) (res result) {
	// 调用函数时必须持有mu锁, 且该函数调用结束时会释放锁

	// 直接覆盖之前记录的chan
	newCh := make(chan result)
	kv.waiCh[opArgs.Identifier] = &newCh
	DPrintf("leader %v identifier %v Seq %v 的请求: 新建管道: %p\n", kv.me, opArgs.Identifier, opArgs.Seq, &newCh)
	kv.mu.Unlock() // Start函数耗时较长, 先解锁

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, opArgs.Identifier)
		close(newCh)
		kv.mu.Unlock()
	}()

	_, _, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		return result{Err: ErrNotLeader, Value: ""}
	}

	// 等待消息到达或超时
	select {
	case msg, success := <-newCh:
		if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			DPrintf("server %v identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v, args.Key=%+v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key)
			res.Err = ErrChanClose
			return
		} else {
			return msg
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
	opArgs := &Op{OpType: OPGet, Seq: args.Seq, Key: args.Key, Identifier: args.Identifier}

	kv.mu.Lock()

	// 先记录是否存在
	if clerkHis, exist := kv.historyMap[args.Identifier]; exist {
		// 客户端标识符存在, 进一步判断seq是否一样
		if clerkHis.LastSeq == args.Seq {
			// seq一样, 直接返回历史记录
			kv.mu.Unlock()
			reply.Err = clerkHis.Err
			reply.Value = clerkHis.Value
			kv.LogInfoReceive(opArgs, 1)
			return
		} else if clerkHis.LastSeq < args.Seq {
			// 此时seq是更新的请求
			kv.LogInfoReceive(opArgs, 0)
		} else {
			// 此时seq是更旧的请求, 由于没有记录更旧的请求结果, 直接panic
			kv.mu.Unlock()
			kv.LogInfoReceive(opArgs, 2)
		}
	} else {
		kv.LogInfoReceive(opArgs, 0)
	}

	// 执行到这里时就是这是一个需要Start的操作
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

	opArgs := &Op{Seq: args.Seq, Key: args.Key, Val: args.Value, Identifier: args.Identifier}
	if args.Op == "Put" {
		opArgs.OpType = OPPut
	} else {
		opArgs.OpType = OPAppend
	}

	kv.mu.Lock()

	// 先记录是否存在
	if clerkHis, exist := kv.historyMap[args.Identifier]; exist {
		// 客户端标识符存在, 进一步判断seq是否相等
		if clerkHis.LastSeq == args.Seq {
			reply.Err = clerkHis.Err
			kv.LogInfoReceive(opArgs, 1)
			return
		} else if clerkHis.LastSeq < args.Seq {
			// 此时seq是更新的请求
			kv.LogInfoReceive(opArgs, 0)
		} else {
			// 此时seq是更旧的请求, 由于没有记录更旧的请求结果, 直接panic
			kv.mu.Unlock()
			kv.LogInfoReceive(opArgs, 2)
			panic(nil)
		}
	} else {
		kv.LogInfoReceive(opArgs, 0)
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

func (kv *KVServer) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {

			op := log.Command.(Op)
			kv.mu.Lock()

			// 需要判断这个log是否需要被再次应用
			var res result
			needApply := false
			if hisMap, exist := kv.historyMap[op.Identifier]; exist {
				if hisMap.LastSeq < op.Seq {
					// 历史记录存在但Seq更小
					needApply = true
				} else {
					// 否则直接使用历史记录
					res = *hisMap
				}
			} else {
				// 历史记录不存在
				needApply = true
			}

			_, isLeader := kv.rf.GetState()

			if needApply {
				// 执行log
				res = kv.DBExecute(&op, isLeader)
				// 更新历史记录
				kv.historyMap[op.Identifier] = &res
			}

			if !isLeader {
				// 不是leader则继续检查下一个log
				kv.mu.Unlock()
				continue
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := kv.waiCh[op.Identifier]
			if !exist {
				// 接收端的通道已经被删除了并且当前节点是 leader, 说明这是重复的请求, 但这种情况不应该出现, 所以panic
				DPrintf("leader %v 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", kv.me, op.Identifier, op.Seq)
				kv.mu.Unlock()
				continue
			}
			kv.mu.Unlock()
			// 发送消息
			*ch <- res
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
	kv.historyMap = make(map[int64]*result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int64]*chan result)

	go kv.ApplyHandler()

	return kv
}
