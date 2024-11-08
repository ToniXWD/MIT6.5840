package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	identifier int64
	seq        uint64
}

func (ck *Clerk) GetSeq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.identifier = nrand()
	ck.seq = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	si := 0
	for {
		args := &GetArgs{Key: key, Seq: ck.GetSeq(), Identifier: ck.identifier, ConfigNum: ck.config.Num}

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					// 正常完成请求
					ClientLog("请求: Get(%v)正常完成: Seq=%v, Identifier=%v, ConfigNum=%v, result=%v", args.Key, args.Seq, args.Identifier, args.ConfigNum, reply.Value)
					return reply.Value
				} else if ok && reply.Err == ErrWrongLeader {
					// 当前节点不是集群的 leader
					ClientLog("请求: Get(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					si = (si + 1) % len(ck.config.Groups[gid])
					continue
				} else if ok && reply.Err == ErrGroupIsInMigrant {
					// 集群正在迁移配置中，先 sleep, 然后访问
					ClientLog("请求: Get(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					time.Sleep(100 * time.Millisecond)
					continue
				} else if ok && reply.Err == ErrWrongConfNum {
					ClientLog("请求: Get(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					ClientLog("config=%+v\n", ck.config)
					// 当前 client 的配置太旧了，需要 break 以更新配置
					break
				} else if ok && (reply.Err == ErrWrongShardForCurGroup) {
					ClientLog("请求: Get(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					ClientLog("config=%+v\n", ck.config)
					// 当前的节点不负责这个 key 的分片，需要 break 以更新配置
					break
				} else if ok && reply.Err == ErrKVWaitForArriving {
					ClientLog("请求: Get(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					// 当前的节点正在迁移数据，需要 break 以更新配置
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	si := 0
	for {
		args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.GetSeq(), Identifier: ck.identifier, ConfigNum: ck.config.Num}

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {

			for {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", args, &reply)
				if ok && (reply.Err == OK) {
					// 正常完成请求
					ClientLog("请求: PutAppend(%v)正常完成: Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, args.Seq, args.Identifier, args.ConfigNum)
					return
				} else if ok && reply.Err == ErrWrongLeader {
					// 当前节点不是集群的 leader
					ClientLog("请求: PutAppend(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v, si=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum, si)
					si = (si + 1) % len(ck.config.Groups[gid])
					continue
				} else if ok && reply.Err == ErrGroupIsInMigrant {
					// 集群正在迁移配置中，先 sleep, 然后访问
					ClientLog("请求: PutAppend(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					time.Sleep(100 * time.Millisecond)
					continue
				} else if ok && reply.Err == ErrWrongConfNum {
					ClientLog("请求: PutAppend(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					ClientLog("config=%+v\n", ck.config)
					// 当前 client 的配置太旧了，需要 break 以更新配置
					break
				} else if ok && (reply.Err == ErrWrongShardForCurGroup) {
					ClientLog("请求: PutAppend(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					ClientLog("config=%+v\n", ck.config)
					// 当前的节点不负责这个 key 的分片，需要 break 以更新配置
					break
				} else if ok && reply.Err == ErrKVWaitForArriving {
					ClientLog("请求: PutAppend(%v)错误: %v, Seq=%v, Identifier=%v, ConfigNum=%v", args.Key, reply.Err, args.Seq, args.Identifier, args.ConfigNum)
					// 当前的节点正在迁移数据，需要 break 以更新配置
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
