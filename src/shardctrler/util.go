package shardctrler

import (
	"log"
	"sort"
	"time"
)

var Debug = false

const HandleOpTimeOut = time.Millisecond * 2000 // 超时为2s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("kv-"+format, a...)
	}
	return
}

func ControlerLog(format string, a ...interface{}) {
	DPrintf("ctl "+format, a...)
}

func get_max_count_len(newConfig *Config) (map_shard_len map[int]int, max_map_gid_len int, max_map_gid_count int) {
	map_shard_len = make(map[int]int)

	max_map_gid_len = 0   // gid包含最多shard的数量
	max_map_gid_count = 0 // 含最多shard的数量的group的数量

	for _, gid := range newConfig.Shards {
		map_shard_len[gid]++
		if map_shard_len[gid] > max_map_gid_len {
			max_map_gid_len = map_shard_len[gid]
		}
	}

	for _, gid := range newConfig.Shards {
		if map_shard_len[gid] == max_map_gid_len {
			max_map_gid_count += 1
		}
	}
	return
}

func CreateNewConfig(configs []Config, newGroups map[int][]string) Config {
	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:    lastConfig.Num + 1,
		Shards: lastConfig.Shards,
		Groups: make(map[int][]string),
	}

	// Combine old and new groups
	for gid, servers := range lastConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	for gid, servers := range newGroups {
		newConfig.Groups[gid] = servers
	}

	// first config shard
	if len(lastConfig.Groups) == 0 {
		gids := make([]int, 0, len(newConfig.Groups))
		for gid := range newConfig.Groups {
			gids = append(gids, gid)
		}

		groupNum := len(gids)
		if groupNum == 0 {
			panic("no groups to assign shards to")
		}

		for shard := 0; shard < NShards; shard++ {
			mapGid := gids[shard%groupNum]
			newConfig.Shards[shard] = mapGid
		}
	} else {
		// Reallocate shards
		map_shard_len, max_map_gid_len, max_map_gid_count := get_max_count_len(&newConfig)

		for new_gid := range newGroups {
			map_shard_len[new_gid] = 0
			idx := 0
			for max_map_gid_len > map_shard_len[new_gid]+1 {
				old_gid := newConfig.Shards[idx]

				if map_shard_len[old_gid] == max_map_gid_len {
					// old_gid标识的这个group分配了最多的shard, 将当前的shard重新分配给新的Group
					newConfig.Shards[idx] = new_gid
					max_map_gid_count -= map_shard_len[old_gid]
					map_shard_len[old_gid]--
					if max_map_gid_count == 0 {
						// 原来映射最多的组都已经被剥夺了一个映射, 需要重新统计
						map_shard_len, max_map_gid_len, max_map_gid_count = get_max_count_len(&newConfig)
					}
				}
				idx++
				idx %= NShards
			}
		}
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

	sort.Ints(remainGids)

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
