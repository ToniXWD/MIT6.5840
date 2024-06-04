package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type OType string

const (
	OPGet     OType = "Get"
	OPPut     OType = "Put"
	OPAppend  OType = "Append"
	OPMigrant OType = "Migrant"
)
const (
	HandleOpTimeOut       = time.Millisecond * 2000 // 超时为2s
	CheckNewConfigTimeOut = time.Millisecond * 2000 // 检查配置更新的超时为2s
	RPCTimeOut            = time.Millisecond * 30   // RPC重发的超时为2s
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
	ConfigNum  int // config number
}

type Result struct {
	LastSeq uint64
	Err     Err
	Value   string
	ResTerm int
}

type MigrantData struct {
	config         shardctrler.Config
	receive_shards map[int]int // shard_idx->old_shard_gid, 映射到原来拥有这个分片的集群
	move_shards    map[int]int // shard_idx->new_shard_gid, 映射到新分配的集群
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm          *shardctrler.Clerk
	config      *shardctrler.Config
	migrantData map[int]*MigrantData
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
func (kv *ShardKV) isConfigLegal(opArgs *Op) (legal bool, res Result) {
	// 调用时必须持有锁
	// legal = true
	// if opArgs.OpType == OPMigrant {
	// 	return
	// }

	// kv.confMu.Lock()
	// defer kv.confMu.Unlock()

	// if kv.config.Num > opArgs.ConfigNum {
	// 	legal = false
	// 	res.Err = ErrOldConfigForClient
	// 	return
	// } else if kv.config.Num < opArgs.ConfigNum {
	// 	legal = false
	// 	res.Err = ErrGroupIsInMigrant
	// 	return
	// } else {
	// 	shard := key2shard(opArgs.Key)
	// 	if kv.config.Shards[shard] != kv.gid {
	// 		legal = false
	// 		res.Err = ErrWrongShardForCurGroup
	// 		ServerLog(kv.gid, "server %v 所属gid = %v, key所属的分片为%v, 而配置文件的映射为: %+v", kv.me, kv.gid, shard, kv.config.Shards)

	// 		return
	// 	}
	// }
	legal = true
	return
}

func (kv *ShardKV) HandleMigrantOp(opArgs *Op) (res Result) {
	_, _, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		ServerLog(kv.gid, "server %v HandleMigrantOp: 拒绝 %s 请求: (%v, %v), 不是 Leader", kv.me, opArgs.OpType, opArgs.Key, opArgs.Val)
		res.Err = ErrWrongLeader
		return
	}
	res.Err = OK
	return
}

func (kv *ShardKV) HandleOp(opArgs *Op) (res Result) {
	// 先判断是否有历史记录
	kv.mu.Lock()
	configLegal, configRes := kv.isConfigLegal(opArgs)
	if !configLegal {
		ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 配置冲突:%v\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, configRes.Err)

		kv.mu.Unlock()
		res.Err = configRes.Err
		return
	}
	// 先判断是否有历史记录
	if hisMap, exist := kv.historyMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		kv.mu.Unlock()
		ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 从历史记录返回\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)
		return *hisMap
	}
	kv.mu.Unlock()

	ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 准备调用Start\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)

	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		ServerLog(kv.gid, "server %v HandleOp: 拒绝 %s 请求: (%v, %v), 不是 Leader", kv.me, opArgs.OpType, opArgs.Key, opArgs.Val)
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
		ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v: 超时", kv.me, opArgs.Identifier, opArgs.Seq)
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
	opArgs := &Op{OpType: OPGet, Seq: args.Seq, Key: args.Key, Identifier: args.Identifier, ConfigNum: args.ConfigNum}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArgs := &Op{Seq: args.Seq, Key: args.Key, Val: args.Value, Identifier: args.Identifier, ConfigNum: args.ConfigNum}
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

func (kv *ShardKV) genNewConfigData(newConfig *shardctrler.Config) int {
	if _, exist := kv.migrantData[newConfig.Num]; exist {
		ServerLog(kv.me, "handleNewConfig: 已经存在的配置更新请求")
		return -1 // 已经有这个新的配置项的记录了吗这是一个重复的请求
	}

	kv.migrantData[newConfig.Num] = &MigrantData{}
	kv.migrantData[newConfig.Num].config = *newConfig

	// 调用这个函数必须持有锁
	receive_shards := make(map[int]int) // shard_idx->old_shard_gid, 映射到原来拥有这个分片的集群
	move_shards := make(map[int]int)    // shard_idx->new_shard_gid, 映射到新分配的集群
	for shard_idx := 0; shard_idx < shardctrler.NShards; shard_idx++ {
		old_shard_gid := kv.config.Shards[shard_idx]
		new_shard_gid := newConfig.Shards[shard_idx]

		if old_shard_gid == kv.gid && new_shard_gid != old_shard_gid {
			// 某个分片被分配给别的集群
			move_shards[shard_idx] = new_shard_gid
		} else if new_shard_gid == kv.gid && new_shard_gid != old_shard_gid {
			// 接受到了一个新的分片
			receive_shards[shard_idx] = old_shard_gid
		}
	}

	kv.migrantData[newConfig.Num].move_shards = move_shards
	kv.migrantData[newConfig.Num].receive_shards = receive_shards

	return newConfig.Num
}

func (kv *ShardKV) ConfigChecker() {
	for !kv.killed() {
		time.Sleep(CheckNewConfigTimeOut)

		latest_config := kv.sm.Query(-1)

		kv.mu.Lock()

		config_num := -1
		if kv.config.Num < latest_config.Num {
			config_num = kv.genNewConfigData(&latest_config)
		}

		kv.mu.Unlock()

		if config_num > 0 {
			ServerLog(kv.gid, "server %v ConfigChecker: 发现更新的配置: %+v", kv.me, latest_config)
			for {
				migrate_op := &Op{OpType: OPMigrant, Seq: uint64(config_num)}
				res := kv.HandleMigrantOp(migrate_op)
				if res.Err == OK || res.Err == ErrWrongLeader {
					// 让Leader来分发配置更改的log
					ServerLog(kv.gid, "server %v ConfigChecker: HandleOp返回结果: %v", kv.me, res.Err)
					break
				} else {
					ServerLog(kv.gid, "server %v ConfigChecker: HandleOp返回错误: %v", kv.me, res.Err)
				}
				time.Sleep(RPCTimeOut)
			}
		}
	}
}

func (kv *ShardKV) ApplyMigrantOp(ConfigNum uint64) {
	// 调用时必须持有锁
	if _, exist := kv.migrantData[int(ConfigNum)]; exist {
		kv.config = &kv.migrantData[int(ConfigNum)].config

		// 可以删除旧的配置项记录了
		for i := 0; i < int(ConfigNum); i++ {
			delete(kv.migrantData, int(ConfigNum))
		}

		if len(kv.migrantData[int(ConfigNum)].receive_shards) > 0 {
			// 如果需要从其他分片中获取数据, 启动该go routine
			go kv.AskForShardData(ConfigNum)
		}
	}
}

func (kv *ShardKV) AskForShardData(ConfigNum uint64) {
	// 请求新的分片数据
	ServerLog(kv.me, "AskForShardData: not implemented!")
}

func (kv *ShardKV) ApplyHandler() {
	time.Sleep(time.Second * 3)
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			kv.mu.Lock()

			if op.OpType == OPMigrant {
				kv.ApplyMigrantOp(op.Seq)
				kv.mu.Unlock()
				continue
			}

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
	kv.sm = shardctrler.MakeClerk(ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.persister = persister
	kv.historyMap = make(map[int64]*Result)
	kv.db = make(map[string]string)
	kv.waiCh = make(map[int]*chan Result)

	cur_config := kv.sm.Query(-1)
	kv.config = &cur_config

	kv.migrantData = map[int]*MigrantData{}

	// 先在启动时检查是否有快照
	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	go kv.ConfigChecker()
	go kv.ApplyHandler()

	return kv
}
