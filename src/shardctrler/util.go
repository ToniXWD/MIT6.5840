package shardctrler

import (
	"log"
	"time"
)

var Debug = true

const HandleOpTimeOut = time.Millisecond * 2000 // 超时为2s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("kv---"+format, a...)
	}
	return
}

func ControlerLog(format string, a ...interface{}) {
	DPrintf("Controler "+format, a...)
}

func CreateNewConfig(configs []Config, newGroups map[int][]string) Config {
	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	// Combine old and new groups
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range newGroups {
		newConfig.Groups[gid] = servers
	}

	gids := make([]int, 0, len(newConfig.Groups))
	for gid := range newConfig.Groups {
		gids = append(gids, gid)
	}

	groupNum := len(gids)
	if groupNum == 0 {
		panic("no groups to assign shards to")
	}

	// Reallocate shards
	for shard := 0; shard < NShards; shard++ {
		mapGid := gids[shard%groupNum]
		newConfig.Shards[shard] = mapGid
	}

	return newConfig
}

func RemoveGidServers(configs []Config, gids []int) Config {
	if len(configs) == 0 {
		panic("len(configs) == 0")
	}

	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	removeGids := make(map[int]struct{})
	for _, gid := range gids {
		removeGids[gid] = struct{}{}
	}

	remainGids := make([]int, 0)
	for gid, servers := range lastConfig.Groups {
		if _, exist := removeGids[gid]; !exist {
			newConfig.Groups[gid] = servers
			remainGids = append(remainGids, gid)
		}
	}

	groupNum := len(remainGids)
	if groupNum == 0 {
		return newConfig
	}

	// Reallocate shards
	for shard := 0; shard < NShards; shard++ {
		mapGid := remainGids[shard%groupNum]
		newConfig.Shards[shard] = mapGid
	}

	return newConfig
}

func MoveShard2Gid(configs []Config, n_shard int, n_gid int) Config {
	if len(configs) == 0 {
		panic("len(configs) == 0")
	}

	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}

	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	for shard, gid := range lastConfig.Shards {
		if shard != n_shard {
			newConfig.Shards[shard] = gid
		} else {
			newConfig.Shards[n_shard] = n_gid
		}
	}
	return newConfig
}

func QueryConfig(configs []Config, num int) Config {
	if len(configs) == 0 {
		return Config{Num: 0}
	}

	lastConfig := configs[len(configs)-1]
	if num == -1 || num >= lastConfig.Num {
		return lastConfig
	}
	return configs[num]
}
