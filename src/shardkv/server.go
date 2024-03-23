package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type OType string

const (
	OPGet    OType = "Get"
	OPPut    OType = "Put"
	OPAppend OType = "Append"
)
const (
	HandleOpTimeOut = time.Millisecond * 1000 // 超时为2s
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

type Result struct {
	LastSeq uint64
	Err     Err
	Value   string
	ResTerm int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db          map[string]string
	persister   *raft.Persister
	lastApplied int                  // 日志中的最高索引
	waiCh       map[int]*chan Result // 映射 startIndex->ch
	historyMap  map[int64]*Result    // 映射 Identifier->*result
}

func (kv *ShardKV) LogInfoReceive(opArgs *Op, logType int) {
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
		ServerLog(kv.gid, "leader %v identifier %v Seq %v %sGet请求: Get(%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key)
	case OPPut:
		ServerLog(kv.gid, "leader %v identifier %v Seq %v %sPut请求: Put(%v,%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key, opArgs.Val)
	case OPAppend:
		ServerLog(kv.gid, "leader %v identifier %v Seq %v %sPut请求: Put(%v,%v),\n", kv.me, opArgs.Identifier, opArgs.Seq, dateStr, opArgs.Key, opArgs.Val)
	}

	if needPanic {
		panic("没有记录更早的请求的结果")
	}
}

func (kv *ShardKV) LogInfoDBExecute(opArgs *Op, err Err, res string) {
	switch opArgs.OpType {
	case OPGet:
		if err != "" {
			ServerLog(kv.gid, "server %v DBExecute: identifier %v Seq %v DB执行Get请求: Get(%v), Err=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, err)
		} else {
			ServerLog(kv.gid, "server %v DBExecute: iidentifier %v Seq %v DB执行Get请求: Get(%v), res=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, res)
		}
	case OPPut:
		if err != "" {
			ServerLog(kv.gid, "server %v DBExecute: iidentifier %v Seq %v DB执行Put请求: Put(%v,%v), Err=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, err)

		} else {
			ServerLog(kv.gid, "server %v DBExecute: iidentifier %v Seq %v DB执行Put请求: Put(%v,%v), res=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, res)
		}
	case OPAppend:
		if err != "" {
			ServerLog(kv.gid, "server %v DBExecute: iidentifier %v Seq %v DB执行Append请求: Put(%v,%v), Err=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, err)
		} else {
			ServerLog(kv.gid, "server %v DBExecute: iidentifier %v Seq %v DB执行Append请求: Put(%v,%v), res=%s\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, opArgs.Val, res)
		}
	}
}

func (kv *ShardKV) DBExecute(op *Op) (res Result) {
	// 调用该函数需要持有锁
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPGet:
		val, exist := kv.db[op.Key]
		if exist {
			kv.LogInfoDBExecute(op, "", val)
			res.Err = OK
			res.Value = val
			return
		} else {
			res.Err = ErrNoKey
			res.Value = ""
			kv.LogInfoDBExecute(op, "", ErrNoKey)
			return
		}
	case OPPut:
		kv.db[op.Key] = op.Val
		kv.LogInfoDBExecute(op, "", kv.db[op.Key])
		res.Err = OK
		return
	case OPAppend:
		val, exist := kv.db[op.Key]
		if exist {
			kv.db[op.Key] = val + op.Val
			kv.LogInfoDBExecute(op, "", kv.db[op.Key])
			res.Err = OK
			return
		} else {
			kv.db[op.Key] = op.Val
			kv.LogInfoDBExecute(op, "", kv.db[op.Key])
			res.Err = OK
			return
		}
	}
	return
}

func (kv *ShardKV) HandleOp(opArgs *Op) (res Result) {
	// 先判断是否有历史记录
	kv.mu.Lock()
	if hisMap, exist := kv.historyMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		kv.mu.Unlock()
		ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 从历史记录返回\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)
		return *hisMap
	}
	kv.mu.Unlock()

	ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 准备调用Start\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)

	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		ServerLog(kv.gid, "server %v 拒绝 %s 请求: (%v, %v), 不是 Leader", kv.me, opArgs.OpType, opArgs.Key, opArgs.Val)
		return Result{Err: ErrWrongLeader, Value: ""}
	}

	kv.mu.Lock()

	// 直接覆盖之前记录的chan
	newCh := make(chan Result)
	kv.waiCh[startIndex] = &newCh
	ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 新建管道: %p\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, &newCh)
	kv.mu.Unlock() // Start函数耗时较长, 先解锁

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	// 等待消息到达或超时
	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		ServerLog(kv.gid, "server %v identifier %v Seq %v: 超时", kv.me, opArgs.Identifier, opArgs.Seq)
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v: HandleOp 成功, %s(%v, %v), res=%v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, res.Value)
			return
		} else if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			// TODO: 是否需要判断消息到达时自己已经不是leader了?
			ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v, args.Key=%+v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key)
			res.Err = ErrChanClose
			return
		} else {
			// term与一开始不匹配, 说明这个Leader可能过期了
			ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v: term与一开始不匹配, 说明这个Leader可能过期了, res.ResTerm=%v, startTerm=%+v", kv.me, opArgs.Identifier, opArgs.Seq, res.ResTerm, startTerm)
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opArgs := &Op{OpType: OPGet, Seq: args.Seq, Key: args.Key, Identifier: args.Identifier}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := &Op{Seq: args.Seq, Key: args.Key, Val: args.Value, Identifier: args.Identifier}
	if args.Op == "Put" {
		opArgs.OpType = OPPut
	} else {
		opArgs.OpType = OPAppend
	}

	res := kv.HandleOp(opArgs)

	reply.Err = res.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) ApplyHandler() {
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			kv.mu.Lock()

			// 如果在follower一侧, 可能这个log包含在快照中, 直接跳过
			if log.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}

			kv.lastApplied = log.CommandIndex

			// 需要判断这个log是否需要被再次应用
			var res Result

			needApply := false
			if hisMap, exist := kv.historyMap[op.Identifier]; exist {
				if hisMap.LastSeq == op.Seq {
					// 历史记录存在且Seq相同, 直接套用历史记录
					res = *hisMap
				} else if hisMap.LastSeq < op.Seq {
					// 否则新建
					needApply = true
				}
			} else {
				// 历史记录不存在
				needApply = true
			}

			if needApply {
				// 执行log
				res = kv.DBExecute(&op)
				res.ResTerm = log.SnapshotTerm

				// 更新历史记录
				kv.historyMap[op.Identifier] = &res
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := kv.waiCh[log.CommandIndex]
			if exist {
				kv.mu.Unlock()
				// 发送消息
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							ServerLog(kv.gid, "leader %v ApplyHandler: 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", kv.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm

					*ch <- res
				}()
				kv.mu.Lock()
			}

			// 每收到一个log就检测是否需要生成快照
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
				// 当达到95%容量时需要生成快照
				snapShot := kv.GenSnapShot()
				kv.rf.Snapshot(log.CommandIndex, snapShot)
			}
			kv.mu.Unlock()
		} else if log.SnapshotValid {
			// 日志项是一个快照
			kv.mu.Lock()
			if log.SnapshotIndex >= kv.lastApplied {
				kv.LoadSnapShot(log.Snapshot)
				kv.lastApplied = log.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) GenSnapShot() []byte {
	// 调用时必须持有锁mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.db)
	e.Encode(kv.historyMap)

	serverState := w.Bytes()
	return serverState
}

func (kv *ShardKV) LoadSnapShot(snapShot []byte) {
	// 调用时必须持有锁mu
	if len(snapShot) == 0 || snapShot == nil {
		ServerLog(kv.gid, "server %v LoadSnapShot: 快照为空", kv.me)
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	tmpDB := make(map[string]string)
	tmpHistoryMap := make(map[int64]*Result)
	if d.Decode(&tmpDB) != nil ||
		d.Decode(&tmpHistoryMap) != nil {
		ServerLog(kv.gid, "server %v LoadSnapShot 加载快照失败\n", kv.me)
	} else {
		kv.db = tmpDB
		kv.historyMap = tmpHistoryMap
		ServerLog(kv.gid, "server %v LoadSnapShot 加载快照成功\n", kv.me)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.persister = persister
	kv.historyMap = make(map[int64]*Result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int]*chan Result)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	// 先在启动时检查是否有快照
	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ApplyHandler()

	return kv
}
