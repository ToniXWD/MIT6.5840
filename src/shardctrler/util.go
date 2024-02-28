package shardctrler

import (
	"log"
	"math"
	"sort"
	"time"
)

var Debug = false
var Special = true

const HandleOpTimeOut = time.Millisecond * 2000 // 超时为2s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("kv-"+format, a...)
	}
	return
}
func SDPrintf(format string, a ...interface{}) (n int, err error) {
	if Special {
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

func get_min_count_len(newConfig *Config) (map_shard_min map[int]interface{}) {

	map_shard_min = make(map[int]interface{})

	map_shard_len := make(map[int]int)

	min_map_gid_len := math.MaxInt // gid包含最少shard的数量

	for _, gid := range newConfig.Shards {
		if _, exist := newConfig.Groups[gid]; exist {
			map_shard_len[gid]++
		}
	}

	for _, count := range map_shard_len {
		min_map_gid_len = int(math.Min(float64(count), float64(min_map_gid_len)))
	}

	for gid, count := range map_shard_len {
		if count == min_map_gid_len {
			map_shard_min[gid] = nil
		}
	}

	return
}

func CreateNewConfig(me int, configs []Config, newGroups map[int][]string) Config {
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

	new_gids := make([]int, 0)
	for gid, servers := range newGroups {
		// group的数量不能超过NShards
		newConfig.Groups[gid] = servers
		new_gids = append(new_gids, gid)
	}

	// first config shard or previous config is empty
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

		for _, new_gid := range new_gids {
			map_shard_len[new_gid] = 0
			idx := 0
			for max_map_gid_len > map_shard_len[new_gid]+1 {
				old_gid := newConfig.Shards[idx]

				if map_shard_len[old_gid] == max_map_gid_len {
					// old_gid标识的这个group分配了最多的shard, 将当前的shard重新分配给新的Group
					newConfig.Shards[idx] = new_gid
					max_map_gid_count -= map_shard_len[old_gid]
					map_shard_len[old_gid]--
					map_shard_len[new_gid]++
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
	SDPrintf("%v", me)

	SDPrintf("%v-CreateNewConfig-lastConfig.Groups= %v, newGroups=%+v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, newGroups, len(lastConfig.Groups))
	SDPrintf("%v-CreateNewConfig-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-CreateNewConfig-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-CreateNewConfig-newConfig.Shards= %v", me, newConfig.Shards)

	// to_delte_gids := make([]int, 0)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			// to_delte_gids = append(to_delte_gids, missing)
			SDPrintf("%v-CreateNewConfig, missing gid= %v", me, missing)
		}
	}

	// for _, missing_gid := range to_delte_gids {
	// 	delete(newConfig.Groups, missing_gid)
	// }

	return newConfig
}

func RemoveGidServers(me int, configs []Config, gids []int) Config {
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
		SDPrintf("%v", me)
		SDPrintf("%v-RemoveGidServers-lastConfig.Groups= %v, gids= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, gids, len(lastConfig.Groups))
		SDPrintf("%v-RemoveGidServers-lastConfig.Shards= %v", me, lastConfig.Shards)

		SDPrintf("%v-RemoveGidServers-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
		SDPrintf("%v-RemoveGidServers-newConfig.Shards= %v", me, newConfig.Shards)
		return newConfig
	}

	// Reallocate shards
	for shard := 0; shard < NShards; shard++ {
		mapGid := remainGids[shard%groupNum]
		newConfig.Shards[shard] = mapGid
	}
	SDPrintf("%v", me)
	SDPrintf("%v-RemoveGidServers-lastConfig.Groups= %v, gids= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, gids, len(lastConfig.Groups))
	SDPrintf("%v-RemoveGidServers-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-RemoveGidServers-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-RemoveGidServers-newConfig.Shards= %v", me, newConfig.Shards)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			SDPrintf("%v-RemoveGidServers, missing gid= %v", me, missing)

			SDPrintf("error")
		}
	}
	return newConfig
}

func MoveShard2Gid(me int, configs []Config, n_shard int, n_gid int) Config {
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
	SDPrintf("%v", me)
	SDPrintf("%v-MoveShard2Gid-lastConfig.Groups= %v, n_shard= %v,n_gid= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, n_shard, n_gid, len(lastConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-MoveShard2Gid-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-newConfig.Shards= %v", me, newConfig.Shards)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			SDPrintf("%v-MoveShard2Gid, missing gid= %v", missing)

			SDPrintf("error")
		}
	}
	return newConfig
}

func QueryConfig(me int, configs []Config, num int) Config {
	if len(configs) == 0 {
		return Config{Num: 0}
	}

	lastConfig := configs[len(configs)-1]
	if num == -1 || num >= lastConfig.Num {
		return lastConfig
	}
	return configs[num]
}
