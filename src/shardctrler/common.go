package shardctrler

import "sort"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.

const (
	NShards = 10
)

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num       int              // config number
	Shards    [NShards]int     // shard -> gid
	Groups    map[int][]string // gid -> servers[]
	CachedGid map[int][]string // gid -> cached_servers[]
}

func (cf *Config) check_nil_map() {
	if cf.CachedGid == nil {
		cf.CachedGid = make(map[int][]string)
	}

	if cf.Groups == nil {
		cf.Groups = make(map[int][]string)
	}
}

func (cf *Config) add_cache(gid int, servers []string) {
	if cf.CachedGid == nil {
		cf.CachedGid = make(map[int][]string)
	}

	if _, exist := cf.Groups[gid]; !exist && len(cf.Groups) == NShards {
		cf.CachedGid[gid] = servers
	}
}

func (cf *Config) remove_cache(gids []int) {
	if cf.CachedGid == nil {
		cf.CachedGid = make(map[int][]string)
		return
	}

	for _, gid := range gids {
		delete(cf.CachedGid, gid)
	}
}

func (cf *Config) remove_group(gids []int) {
	for _, gid := range gids {
		delete(cf.Groups, gid)
	}
	for idx, gid := range cf.Shards {
		if _, exist := cf.Groups[gid]; !exist {
			cf.Shards[idx] = 0
		}
	}
}

func (cf *Config) move_cache() {
	if cf.CachedGid == nil {
		cf.CachedGid = make(map[int][]string)
		return
	}

	move_total := NShards - len(cf.Groups)

	move_idx := 0

	cache_gids := make([]int, 0, len(cf.CachedGid))
	for cache_key := range cf.CachedGid {
		cache_gids = append(cache_gids, cache_key)
	}

	sort.Ints(cache_gids) // 需要按照顺序移动到Groups

	for move_idx < move_total && move_idx < len(cache_gids) {
		move_key := cache_gids[move_idx]
		cf.Groups[move_key] = cf.CachedGid[move_key]
		delete(cf.CachedGid, move_key)

		move_idx++
	}
}

const (
	OK = "OK"
)

const (
	ErrNotLeader       = "NotLeader"
	ErrKeyNotExist     = "KeyNotExist"
	ErrHandleOpTimeOut = "HandleOpTimeOut"
	ErrChanClose       = "ChanClose"
	ErrLeaderOutDated  = "LeaderOutDated"
	ERRRPCFailed       = "RPCFailed"
)

type Err string

type JoinArgs struct {
	Servers    map[int][]string // new GID -> servers mappings
	Seq        uint64
	Identifier int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs       []int
	Seq        uint64
	Identifier int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard      int
	GID        int
	Seq        uint64
	Identifier int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num        int // desired config number
	Seq        uint64
	Identifier int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
