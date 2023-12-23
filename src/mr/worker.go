package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		// 循环请求
		replyMsg := CallForTask()

		switch replyMsg.MsgType {
		case MapTaskAlloc:
			err := HandleMapTask(replyMsg, mapf)
			if err == nil {
				_ = CallForReportStatus(MapSuccess, replyMsg.TaskID)
			} else {
				// log.Println("Worker: Map Task failed")
				_ = CallForReportStatus(MapFailed, replyMsg.TaskID)
			}
		case ReduceTaskAlloc:
			err := HandleReduceTask(replyMsg, reducef)
			if err == nil {
				_ = CallForReportStatus(ReduceSuccess, replyMsg.TaskID)
			} else {
				// log.Println("Worker: Map Task failed")
				_ = CallForReportStatus(ReduceFailed, replyMsg.TaskID)
			}
		case Wait:
			time.Sleep(time.Second * 10)
		case Shutdown:
			os.Exit(0)
		}
		time.Sleep(time.Second)
	}
}

func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kva {
		redId := ihash(kv.Key) % reply.NReduce
		if encoders[redId] == nil {
			tempFile, err := ioutil.TempFile("", fmt.Sprintf("mr-map-tmp-%d", redId))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			tempFiles[redId] = tempFile
			encoders[redId] = json.NewEncoder(tempFile)
		}
		err := encoders[redId].Encode(&kv)
		if err != nil {
			return err
		}
	}

	for i, file := range tempFiles {
		if file != nil {
			fileName := file.Name()
			file.Close()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskID, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}

	return nil
}

func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) error {
	key_id := reply.TaskID

	k_vs := map[string][]string{}

	fileList, err := ReadSpecificFile(key_id, "./")

	if err != nil {
		return err
	}

	// 整理所有的中间文件
	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			k_vs[kv.Key] = append(k_vs[kv.Key], kv.Value)
		}
		file.Close()
	}

	// 获取所有的键并排序
	var keys []string
	for k := range k_vs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	oname := "mr-out-" + strconv.Itoa(reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, k_vs[key])
		_, err := fmt.Fprintf(ofile, "%v %v\n", key, output)
		if err != nil {
			return err
		}
	}

	DelFileByReduceId(reply.TaskID, "./")

	return nil
}

func CallForReportStatus(succesType MsgType, taskID int) error {
	// 报告Task执行情况
	// declare an argument structure.
	args := MessageSend{
		MsgType: succesType,
		TaskID:  taskID,
	}
	// if succesType == MapSuccess {
	// log.Printf("Worker: Report Map success: %v", taskID)
	// } else {
	// log.Printf("Worker: Report Reduce success: %v", taskID)
	// }

	err := call("Coordinator.NoticeResult", &args, nil)
	// if err != nil {
	// 	fmt.Printf("Worker: Report success failed: %s\n", err.Error())
	// }
	return err
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallForTask() *MessageReply {
	// 请求一个Task
	// declare an argument structure.
	args := MessageSend{
		MsgType: AskForTask,
	}

	// declare a reply structure.
	reply := MessageReply{}

	// send the RPC request, wait for the reply.
	err := call("Coordinator.AskForTask", &args, &reply)
	if err == nil {
		// fmt.Printf("TaskName %v, NReduce %v, taskID %v\n", reply.TaskName, reply.NReduce, reply.TaskID)
		return &reply
	} else {
		// log.Println(err.Error())
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// log.Fatal("dialing:", err)
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)

	return err
}
