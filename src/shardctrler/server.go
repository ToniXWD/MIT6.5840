package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	waiCh      map[int]*chan Result // 映射 startIndex->ch
	historyMap map[int64]*Result    // 映射 Identifier->*result

	configs []Config // indexed by config num
}

type OType string

const (
	OPJoin  OType = "Join"
	OPLeave OType = "Leave"
	OPMove  OType = "Move"
	OPQuery OType = "Query"
)

type Op struct {
	OpType OType
	// JoinArgs
	Servers map[int][]string
	// LeaveArgs
	GIDs []int
	// MoveArgs
	Shard int
	GID   int
	// QueryArgs
	Num int
	// All
	Seq        uint64
	Identifier int64
}

type Result struct {
	LastSeq uint64
	Config  Config
	Err     Err
	ResTerm int
}

func (sc *ShardCtrler) CheckAppendConfig(newConfig Config) {
	if newConfig.Num > sc.configs[len(sc.configs)-1].Num {
		sc.configs = append(sc.configs, newConfig)
	}
}

func (sc *ShardCtrler) ConfigExecute(op *Op) (res Result) {
	// 调用时要求持有锁
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPJoin:
		newConfig := CreateNewConfig(sc.me, sc.configs, op.Servers)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPLeave:
		newConfig := RemoveGidServers(sc.me, sc.configs, op.GIDs)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPMove:
		newConfig := MoveShard2Gid(sc.me, sc.configs, op.Shard, op.GID)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPQuery:
		rConfig := QueryConfig(sc.me, sc.configs, op.Num)
		res.Config = rConfig
		res.Err = ""
	}
	return
}

func (sc *ShardCtrler) HandleOp(opArgs *Op) (res Result) {
	// 先判断是否有历史记录
	sc.mu.Lock()
	if hisMap, exist := sc.historyMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		sc.mu.Unlock()
		// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 从历史记录返回\n", sc.me, opArgs.Identifier, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num)
		return *hisMap
	}
	sc.mu.Unlock() // Start函数耗时较长, 先解锁

	// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 准备调用Start\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num)

	startIndex, startTerm, isLeader := sc.rf.Start(*opArgs)
	if !isLeader {
		ControlerLog("%v HandleOp: Start发现不是Leader", sc.me)
		return Result{Err: ErrNotLeader}
	}

	sc.mu.Lock()

	// 直接覆盖之前记录的chan
	newCh := make(chan Result)
	sc.waiCh[startIndex] = &newCh
	// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 新建管道: %p\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num, &newCh)
	sc.mu.Unlock() // Start函数耗时较长, 先解锁

	defer func() {
		sc.mu.Lock()
		delete(sc.waiCh, startIndex)
		close(newCh)
		sc.mu.Unlock()
	}()

	// 等待消息到达或超时
	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		ControlerLog("%v HandleOp: identifier %v Seq %v: 超时", sc.me, opArgs.Identifier, opArgs.Seq)
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			// ControlerLog("%v HandleOp: identifier %v Seq %v, HandleOp %s 成功\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType)
			return
		} else if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			// TODO: 是否需要判断消息到达时自己已经不是leader了?
			ControlerLog("%v HandleOp: identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType)
			res.Err = ErrChanClose
			return
		} else {
			// term与一开始不匹配, 说明这个Leader可能过期了
			ControlerLog("%v HandleOp: identifier %v Seq %v: term与一开始不匹配, 说明这个Leader可能过期了, res.ResTerm=%v, startTerm=%+v", sc.me, opArgs.Identifier, opArgs.Seq, res.ResTerm, startTerm)
			res.Err = ErrLeaderOutDated
			return
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	opArgs := &Op{OpType: OPJoin, Seq: args.Seq, Identifier: args.Identifier, Servers: args.Servers}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	opArgs := &Op{OpType: OPLeave, Seq: args.Seq, Identifier: args.Identifier, GIDs: args.GIDs}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	opArgs := &Op{OpType: OPMove, Seq: args.Seq, Identifier: args.Identifier, Shard: args.Shard, GID: args.GID}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	opArgs := &Op{OpType: OPQuery, Seq: args.Seq, Identifier: args.Identifier, Num: args.Num}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
	reply.Config = res.Config
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
	Debug = false
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			sc.mu.Lock()

			// ControlerLog("%v ApplyHandler 收到log: identifier %v Seq %v, type %v", sc.me, op.Identifier, op.Seq, op.OpType)

			// 需要判断这个log是否需要被再次应用
			var res Result

			needApply := false
			if hisMap, exist := sc.historyMap[op.Identifier]; exist {
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
				if op.OpType != OPQuery {
					ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s, Shard=%v) 操作前的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Shard, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
					// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 操作前的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Servers, op.GIDs, op.Shard, op.GID, op.Num, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
				}
				res = sc.ConfigExecute(&op)
				if op.OpType != OPQuery {
					ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s, Shard=%v) 操作后的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Shard, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
					// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 结束后的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Servers, op.GIDs, op.Shard, op.GID, op.Num, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
				}

				res.ResTerm = log.SnapshotTerm

				// 更新历史记录
				sc.historyMap[op.Identifier] = &res
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := sc.waiCh[log.CommandIndex]
			sc.mu.Unlock()
			if exist {
				// 发送消息
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							ControlerLog("leader %v ApplyHandler: 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", sc.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm

					*ch <- res
				}()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.historyMap = make(map[int64]*Result)
	sc.waiCh = make(map[int]*chan Result)

	go sc.ApplyHandler()

	return sc
}
