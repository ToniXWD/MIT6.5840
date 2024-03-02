package shardctrler

import (
	"log"
	"math"
	"sort"
	"time"
)

var Debug = false
var Special = false

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

func deep_copy_map(oldConfig *Config, newConfig *Config) {
	oldConfig.check_nil_map()
	newConfig.check_nil_map()

	for gid, group := range oldConfig.Groups {
		newConfig.Groups[gid] = group
	}

	for gid, group := range oldConfig.CachedGid {
		newConfig.CachedGid[gid] = group
	}
}

func get_max_map_len_count(newConfig *Config) (map_shard_len map[int]int, max_map_gid_len int, max_map_gid_count int) {
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

func get_min_arr(newConfig *Config) (map_shard_min []int) {

	map_shard_min = make([]int, 0)

	map_shard_len := make(map[int]int)

	min_map_gid_len := math.MaxInt // gid包含最少shard的数量

	for _, gid := range newConfig.Shards {
		if _, exist := newConfig.Groups[gid]; exist {
			map_shard_len[gid]++
		}
	}

	// Groups中没有映射到的gid置为0
	for gid := range newConfig.Groups {
		if _, exist := map_shard_len[gid]; !exist {
			map_shard_len[gid] = 0
		}
	}

	for _, count := range map_shard_len {
		min_map_gid_len = int(math.Min(float64(count), float64(min_map_gid_len)))
	}

	for gid, count := range map_shard_len {
		if count == min_map_gid_len {
			map_shard_min = append(map_shard_min, gid)
		}
	}
	// 必须确保有序
	sort.Ints(map_shard_min)

	return
}

func CreateNewConfig(me int, configs []Config, newGroups map[int][]string) Config {
	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:       lastConfig.Num + 1,
		Shards:    lastConfig.Shards,
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// Combine old and new groups
	deep_copy_map(&lastConfig, &newConfig)

	// 要确保加入cache的gid在所有节点上相同, 所以要求有序
	total_new_gids := make([]int, 0)
	for gid := range newGroups {
		total_new_gids = append(total_new_gids, gid)
	}
	sort.Ints(total_new_gids)

	new_gids := make([]int, 0)

	for _, gid := range total_new_gids {
		if len(newConfig.Groups) < NShards {
			// group的数量不能超过NShards
			newConfig.Groups[gid] = newGroups[gid]
			new_gids = append(new_gids, gid)
		} else {
			newConfig.add_cache(gid, newGroups[gid])
		}
	}

	// first config shard or previous config is empty
	if len(lastConfig.Groups) == 0 {
		for shard := 0; shard < NShards; shard++ {
			idx := shard % len(new_gids)
			newConfig.Shards[shard] = new_gids[idx]
		}
	} else {
		// Reallocate shards
		map_shard_len, max_map_gid_len, max_map_gid_count := get_max_map_len_count(&newConfig)

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
						map_shard_len, max_map_gid_len, max_map_gid_count = get_max_map_len_count(&newConfig)
					}
				}
				idx++
				idx %= NShards
			}
		}
	}
	SDPrintf("%v", me)

	SDPrintf("%v-CreateNewConfig-lastConfig.Groups= %v, newGroups=%+v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, newGroups, len(lastConfig.Groups))
	SDPrintf("%v-CreateNewConfig-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-CreateNewConfig-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-CreateNewConfig-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-CreateNewConfig-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))
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
		Num:       lastConfig.Num + 1,
		Shards:    lastConfig.Shards,
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// 深复制Group
	deep_copy_map(&lastConfig, &newConfig)

	// 先移除缓冲区的gid
	newConfig.remove_cache(gids)

	// 再从Groups移除gid, 同时将shard对应的映射置为0
	newConfig.remove_group(gids)

	// 如果缓冲区有剩余的gid, 移动到Groups, 注意要注意顺序
	newConfig.move_cache()

	// 要确保有序
	min_gid_arr := get_min_arr(&newConfig)

	if len(newConfig.Groups) > 0 {
		for shard, map_gid := range newConfig.Shards {
			if map_gid != 0 {
				continue
			}
			newConfig.Shards[shard] = min_gid_arr[0]
			min_gid_arr = min_gid_arr[1:]

			if len(min_gid_arr) == 0 {
				min_gid_arr = get_min_arr(&newConfig)
			}
		}
	} else {
		newConfig.Shards = [NShards]int{}
	}

	SDPrintf("%v", me)
	SDPrintf("%v-RemoveGidServers-lastConfig.Groups= %v, gids= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, gids, len(lastConfig.Groups))
	SDPrintf("%v-RemoveGidServers-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-RemoveGidServers-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-RemoveGidServers-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-RemoveGidServers-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))

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
		Num:       lastConfig.Num + 1,
		Shards:    [NShards]int{},
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// 深复制Group
	deep_copy_map(&lastConfig, &newConfig)

	for shard, gid := range lastConfig.Shards {
		if shard != n_shard {
			newConfig.Shards[shard] = gid
		} else {
			newConfig.Shards[n_shard] = n_gid
		}
	}

	SDPrintf("%v", me)
	SDPrintf("%v-MoveShard2Gid-lastConfig.Groups= %v, n_shard= %v,n_gid= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, n_shard, n_gid, len(lastConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-MoveShard2Gid-lastConfig.Shards= %v", me, lastConfig.Shards)
	SDPrintf("%v-MoveShard2Gid-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-MoveShard2Gid-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))
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
