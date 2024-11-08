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

type ShardDB struct {
	DB         map[string]string // 该分片的数据
	ConfigNum  int               // 该分片目前的配置序列号
	HistoryMap map[int64]*Result // 映射 Identifier->*result
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
	prev_config *shardctrler.Config // ! 目前只允许 2 个新老配置文件进行迁移和调整，即配置文件变动不频繁
	// TODO: 如果配置变更频繁，prev_config 需要更换为一个数组队列
	// migrantData map[int]*UpdateInfo
	db          map[int]*ShardDB // shard->db
	prev_db     map[int]*ShardDB // shard->db, TODO: 如果配置变更频繁，prev_db 需要更换为一个数组队列
	persister   *raft.Persister
	lastApplied int                  // 日志中的最高索引
	waiCh       map[int]*chan Result // 映射 startIndex->ch
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
	shard_id := key2shard(op.Key)
	shard_db := kv.db[shard_id].DB // TODO: 需要确保该分片的 db 存在
	switch op.OpType {
	case OPGet:
		val, exist := shard_db[op.Key]
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
		shard_db[op.Key] = op.Val
		kv.LogInfoDBExecute(op, "", shard_db[op.Key])
		res.Err = OK
		return
	case OPAppend:
		val, exist := shard_db[op.Key]
		if exist {
			shard_db[op.Key] = val + op.Val
			kv.LogInfoDBExecute(op, "", shard_db[op.Key])
			res.Err = OK
			return
		} else {
			shard_db[op.Key] = op.Val
			kv.LogInfoDBExecute(op, "", shard_db[op.Key])
			res.Err = OK
			return
		}
	}
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

// 检查分片配置文件序列号和分片是否合法
// 必须持有锁
func (kv *ShardKV) isReqLegal(opArgs *Op) (bool, *Result) {
	if kv.config.Shards[opArgs.Shard] != kv.gid {
		ServerLog(kv.gid, "server %v isReqLegal: identifier %v Seq %v 的请求: key=%v, 当前配置: %+v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.Key, kv.config)
		return false, &Result{Err: ErrWrongShardForCurGroup}
	}
	if kv.db[opArgs.Shard] == nil {
		return false, &Result{Err: ErrKVWaitForArriving} // 分片的的 kv 数据还没有从其他集群复制过来
	}
	return true, nil
}

func (kv *ShardKV) HandleOp(opArgs *Op) (res Result) {
	// 先查询次请求是否合法 (主要是当前集群是否负责处理该分片，以及配置序列号的校验)
	kv.mu.Lock() // TODO: 是否考虑专门为配置文件设一把锁
	configLegal, configRes := kv.isReqLegal(opArgs)
	if !configLegal {
		ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 配置冲突:%v, 当前配置序列号: %v, 请求的配置序列号: %v, 请求所属分片: %v, 配置映射: %+v, 旧配置: %+v\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, configRes.Err, kv.config.Num, opArgs.ConfigNum, opArgs.Shard, kv.config.Shards, kv.prev_config)

		kv.mu.Unlock()
		res.Err = configRes.Err
		return
	}
	// 先判断是否有历史记录
	hisRes := kv.queryHistory(opArgs)
	if hisRes != nil {
		kv.mu.Unlock()
		return *hisRes
	}

	ServerLog(kv.gid, "server %v HandleOp: identifier %v Seq %v 的请求: %s(%v, %v) 准备调用Start\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)

	// 等待 raft 同步 log
	kv.mu.Unlock()
	return kv.raftExecute(opArgs)
}

// 调用该函数时必须持有锁
func (kv *ShardKV) queryHistory(opArgs *Op) *Result {
	shard_id := key2shard(opArgs.Key)
	if hisMap, exist := kv.db[shard_id].HistoryMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		ServerLog(kv.gid, "server %v queryHistory: identifier %v Seq %v 的请求: %s(%v, %v) 从历史记录返回\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val)
		return hisMap
	}
	return nil
}

// 完成一个 raft 的 log 的同步
// 该函数自己会申请锁，调用时不需要锁
func (kv *ShardKV) raftExecute(opArgs *Op) (res Result) {
	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		ServerLog(kv.gid, "server %v raftExecute: 拒绝 %s 请求: (%v, %v), 不是 Leader", kv.me, opArgs.OpType, opArgs.Key, opArgs.Val)
		return Result{Err: ErrWrongLeader, Value: ""}
	}
	kv.mu.Lock()

	// 直接覆盖之前记录的 chan
	newCh := make(chan Result)
	kv.waiCh[startIndex] = &newCh
	ServerLog(kv.gid, "server %v raftExecute: identifier %v Seq %v 的请求: %s(%v, %v) 新建管道: %p\n", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, &newCh)
	kv.mu.Unlock() // Start 函数耗时较长，先解锁

	defer func() {
		kv.mu.Lock()
		delete(kv.waiCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	res = kv.waitChOrTimeout(startTerm, opArgs, &newCh)
	return res
}

// 从通道中收取一个 Result
// 调用时不能持有锁
func (kv *ShardKV) waitChOrTimeout(startTerm int, opArgs *Op, newCh *chan Result) (res Result) {
	// 等待消息到达或超时
	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		ServerLog(kv.gid, "server %v waitChOrTimeout: identifier %v Seq %v: 超时", kv.me, opArgs.Identifier, opArgs.Seq)
		return
	case msg, success := <-*newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			ServerLog(kv.gid, "server %v waitChOrTimeout: identifier %v Seq %v: HandleOp 成功, %s(%v, %v), res=%v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key, opArgs.Val, res.Value)
			return
		} else if !success {
			// 通道已经关闭，有另一个协程收到了消息 或 通道被更新的 RPC 覆盖
			// TODO: 是否需要判断消息到达时自己已经不是 leader 了？
			ServerLog(kv.gid, "server %v waitChOrTimeout: identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v, args.Key=%+v", kv.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Key)
			res.Err = ErrChanClose
			return
		} else {
			// term 与一开始不匹配，说明这个 Leader 可能过期了
			ServerLog(kv.gid, "server %v waitChOrTimeout: identifier %v Seq %v: term与一开始不匹配, 说明这个Leader可能过期了, res.ResTerm=%v, startTerm=%+v", kv.me, opArgs.Identifier, opArgs.Seq, res.ResTerm, startTerm)
			res.Err = ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	shard_id := key2shard(args.Key)
	opArgs := &Op{OpType: OPGet, Shard: shard_id, Seq: args.Seq, Key: args.Key, Identifier: args.Identifier, ConfigNum: args.ConfigNum}

	res := kv.HandleOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	shard_id := key2shard(args.Key)
	opArgs := &Op{Key: args.Key, Seq: args.Seq, Shard: shard_id, Val: args.Value, Identifier: args.Identifier, ConfigNum: args.ConfigNum}
	if args.Op == "Put" {
		opArgs.OpType = OPPut
	} else {
		opArgs.OpType = OPAppend
	}

	res := kv.HandleOp(opArgs)

	reply.Err = res.Err
}

func (kv *ShardKV) AskShardRpc(args *AskShardArgs, reply *AskShardReply) {
	reply.ConfigNum = kv.config.Num

	if args.ConfigNum != kv.config.Num {
		// 配置序号不同
		reply.Err = ErrWrongConfNum
		return
	}

	reply.ShardDBs = make(map[int]*ShardDB)

	for _, shard_idx := range args.ShardIdx {
		reply.ShardDBs[shard_idx] = kv.prev_db[shard_idx]
		// kv.prev_db[shard_idx]=nil // 如果考虑到 RPC 失败，还需要保留旧的 kv.prev_db[shard_idx]
	}
	reply.Err = OK
}

// FIXME: 这个函数可能需要修改
func (kv *ShardKV) sendAskShardRpc(ConfigNum uint64, old_gid int, shardIdx []int) (*AskShardReply, bool) {
	var servers []string
	ok := false
	if servers, ok = kv.prev_config.Groups[old_gid]; !ok {
		ServerLog(kv.gid, "server %v sendAskShardRpc: 获取集群信息错误, gid= %v", kv.me, old_gid)
		return nil, false
	}

	args := &AskShardArgs{
		ConfigNum: int(ConfigNum),
		ShardIdx:  shardIdx,
		SourceGid: kv.gid,
	}
	si := 0
	for {
		target_end := kv.make_end(servers[si])
		var reply AskShardReply
		ok := target_end.Call("ShardKV.AskShardRpc", args, &reply)
		if ok && (reply.Err == OK) {
			// 正常完成请求
			ServerLog(kv.gid, "请求: sendAskShardRpc 正常完成: remote gid=%v, remote server=%v, ConfigNum=%v, shardIdx=%v", old_gid, servers[si], ConfigNum, shardIdx)
			return &reply, true
		} else if ok && reply.Err == ErrWrongConfNum {
			if reply.ConfigNum > kv.config.Num {
				// 如果接收者返回的配置序号比当前配置序号新，则说明接收者已经是最新的配置
				// 则此时的配置更新请求已经可以作废
				return nil, true
			} else {
				// 等待一会后重试
				time.Sleep(RPCTimeOut)
			}
		}
		si = (si + 1) % len(servers)
	}
}

func (kv *ShardKV) AskForShard(gid int, shard_idxs []int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply, ok := kv.sendAskShardRpc(uint64(kv.config.Num), gid, shard_idxs)

	if ok {
		if reply == nil {
			ServerLog(kv.gid, "server %v AskForShard: 请求: sendAskShardRpc 成功, 但 reply 为 nil, gid=%v, shard_idxs=%v", kv.me, gid, shard_idxs)
			return
		}
		ServerLog(kv.gid, "server %v AskForShard: 请求: sendAskShardRpc 成功, 从 gid=%v 获取到的分片: %+v", kv.me, gid, shard_idxs)
		for shardIdx, shardDB := range reply.ShardDBs {
			kv.db[shardIdx] = shardDB
			if kv.db[shardIdx] == nil {
				// 如果另一个分配的数据也是 nil, 需要初始化一个空 map
				//  因为 nil 的语义是：1) 数据还没有从其他集群复制过来 2) 当前集群不负责该分片
				kv.db[shardIdx] = &ShardDB{
					DB:         make(map[string]string),
					ConfigNum:  kv.config.Num,
					HistoryMap: make(map[int64]*Result),
				}
			}
		}
	} else {
		ServerLog(kv.gid, "server %v AskForShard: 请求: sendAskShardRpc 失败, gid=%v, shard_idxs=%v", kv.me, gid, shard_idxs)
	}
}

// 调用时必须持有锁
func (kv *ShardKV) HandleNewConf(op *Op) {
	// TODO: 实现这个函数
	latest_config := op.NewConfig
	if latest_config.Num <= kv.config.Num {
		// 如果当前配置已经是最新的，则不需要更新
		return
	}

	// TODO: 需要判断 prev_config 是否已经完全完成了迁移任务，这里假设已经完成
	kv.prev_config = kv.config
	kv.config = latest_config
	kv.prev_db = kv.db
	kv.db = make(map[int]*ShardDB)

	if kv.prev_config.Num == 0 {
		// 序号为 0 的是初始配置文件，里面没有分片信息，不需要迁移
		// 但需要初始化分配的数据库使其不为 nil
		kv.initShardDB(kv.config)
		return
	}

	gshardIdxs := make(map[int][]int) // gid -> shard_idxs

	for shard_idx := 0; shard_idx < shardctrler.NShards; shard_idx++ {
		if kv.prev_config.Shards[shard_idx] == kv.gid && kv.config.Shards[shard_idx] == kv.gid {
			// 如果当前分片在两个配置文件中都负责，则不需要迁移数据
			kv.db[shard_idx] = deepCopyShardDB(kv.prev_db[shard_idx], true)
			kv.prev_db[shard_idx] = nil
		} else if kv.prev_config.Shards[shard_idx] != kv.gid && kv.config.Shards[shard_idx] == kv.gid {
			// 新配置文件负责但老配置文件不负责，需要迁移数据
			ServerLog(kv.gid, "server %v HandleNewConf: 需要迁移分片数据: shard_idx=%v", kv.me, shard_idx)
			kv.db[shard_idx] = nil
			gshardIdxs[kv.prev_config.Shards[shard_idx]] = append(gshardIdxs[kv.prev_config.Shards[shard_idx]], shard_idx)
		} else {
			// 在新配置文件中不负责
			kv.db[shard_idx] = nil
		}
	}

	ServerLog(kv.gid, "server %v HandleNewConf: 当前配置: %+v", kv.me, kv.config)
	ServerLog(kv.gid, "server %v HandleNewConf: 旧配置: %+v", kv.me, kv.prev_config)
	ServerLog(kv.gid, "server %v HandleNewConf: 需要迁移的数据: %+v", kv.me, gshardIdxs)

	for gid, shard_idxs := range gshardIdxs {
		// 开启数据迁移请求的 RPC
		go kv.AskForShard(gid, shard_idxs)
	}
}

// 不需要持有锁
func (kv *ShardKV) ConfigChecker() {
	for !kv.killed() {
		time.Sleep(CheckNewConfigTimeOut)

		latest_config := kv.sm.Query(-1)

		need_update := false

		kv.mu.Lock()
		if kv.config.Num < latest_config.Num {
			need_update = true
		}
		kv.mu.Unlock()

		if !need_update {
			continue
		}

		// 只有 leader 需要检查更新的配置
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		ServerLog(kv.gid, "server %v ConfigChecker: 发现更新的配置: %+v", kv.me, latest_config)
		for {
			migrate_op := &Op{OpType: OPNewConf, NewConfig: &latest_config}
			res := kv.HandleMigrantOp(migrate_op)
			if res.Err == OK || res.Err == ErrWrongLeader {
				// 让 Leader 来分发配置更改的 log
				ServerLog(kv.gid, "server %v ConfigChecker: HandleOp返回结果: %v", kv.me, res.Err)
				break
			} else {
				ServerLog(kv.gid, "server %v ConfigChecker: HandleOp返回错误: %v", kv.me, res.Err)
			}
			time.Sleep(RPCTimeOut)
		}
	}
}

func (kv *ShardKV) ApplyHandler() {
	time.Sleep(time.Second * 3)
	for !kv.killed() {
		log := <-kv.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			kv.mu.Lock()

			switch op.OpType {
			case OPNewConf:
				if kv.config.Num >= op.NewConfig.Num {
					// 如果当前配置已经是最新的，则不需要更新
					kv.mu.Unlock()
					continue
				}
				kv.HandleNewConf(&op)
				kv.mu.Unlock()
				continue

			default:
				// 如果在 follower 一侧，可能这个 log 包含在快照中，直接跳过
				if log.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}

				kv.lastApplied = log.CommandIndex

				// 需要判断这个 log 是否需要被再次应用
				var res Result
				needApply := false

				isLegal, legal_res := kv.isReqLegal(&op)
				if !isLegal {
					res.Err = legal_res.Err
				} else {
					shard_id := key2shard(op.Key)
					shard_db := kv.db[shard_id]

					if hisMap, exist := shard_db.HistoryMap[op.Identifier]; exist {
						if hisMap.LastSeq == op.Seq {
							// 历史记录存在且 Seq 相同，直接套用历史记录
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
						// 执行 log
						res = kv.DBExecute(&op)
						res.ResTerm = log.SnapshotTerm

						// 更新历史
						kv.db[shard_id].HistoryMap[op.Identifier] = &res
					}
				}

				// Leader 还需要额外通知 handler 处理 clerk 回复
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

				// 每收到一个 log 就检测是否需要生成快照
				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate/100*95 {
					// 当达到 95% 容量时需要生成快照
					snapShot := kv.GenSnapShot()
					ServerLog(kv.gid, "server %v ApplyHandler: 生成快照: %v", kv.me, snapShot)
					kv.rf.Snapshot(log.CommandIndex, snapShot)
				}
				kv.mu.Unlock()
			}

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
	// 调用时必须持有锁 mu
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	curData := SnapshotData{
		DB:          kv.db,
		Prev_DB:     kv.prev_db,
		Config:      kv.config,
		Prev_Config: kv.prev_config,
	}
	err := e.Encode(curData)
	if err != nil {
		ServerLog(kv.gid, "server %v GenSnapShot 生成快照失败: %v", kv.me, err)
		ServerLog(kv.gid, "server %v curData: %+v", kv.me, curData)
	}

	serverState := w.Bytes()
	return serverState
}

func (kv *ShardKV) LoadSnapShot(snapShot []byte) {
	// 调用时必须持有锁 mu
	if len(snapShot) == 0 || snapShot == nil {
		ServerLog(kv.gid, "server %v LoadSnapShot: 快照为空", kv.me)
		return
	}

	r := bytes.NewBuffer(snapShot)
	d := labgob.NewDecoder(r)

	curData := SnapshotData{}

	if err := d.Decode(&curData); err != nil {
		ServerLog(kv.gid, "server %v LoadSnapShot 加载快照失败\n", kv.me)
	} else {
		kv.db = curData.DB
		kv.prev_db = curData.Prev_DB
		kv.config = curData.Config
		kv.prev_config = curData.Prev_Config

		ServerLog(kv.gid, "server %v LoadSnapShot 加载快照成功\n", kv.me)
	}
}

func (kv *ShardKV) initShardDB(conf *shardctrler.Config) {
	for shard_idx := 0; shard_idx < shardctrler.NShards; shard_idx++ {
		if conf.Shards[shard_idx] == kv.gid {
			shardDB := &ShardDB{
				DB:         make(map[string]string),
				ConfigNum:  conf.Num,
				HistoryMap: make(map[int64]*Result),
			}
			kv.db[shard_idx] = shardDB
		} else {
			kv.db[shard_idx] = nil
		}
	}
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

	kv.db = make(map[int]*ShardDB)
	kv.prev_db = make(map[int]*ShardDB)

	kv.waiCh = make(map[int]*chan Result)

	// kv.migrantData = map[int]*UpdateInfo{}

	// 先在启动时检查是否有快照
	kv.mu.Lock()
	kv.LoadSnapShot(persister.ReadSnapshot())
	kv.mu.Unlock()

	cur_config := kv.sm.Query(-1)
	kv.prev_config = &cur_config
	kv.config = &cur_config
	kv.initShardDB(&cur_config) // 初始化分片数据库，只初始化当前节点负责的分片，其他分片为 nil

	go kv.ConfigChecker()
	go kv.ApplyHandler()

	return kv
}
