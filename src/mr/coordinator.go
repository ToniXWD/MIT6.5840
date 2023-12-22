package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskStatus int

const (
	idle     taskStatus = iota // 闲置未分配
	running                    // 正在运行
	finished                   // 完成
	failed                     //失败
)

type MapTaskInfo struct {
	TaskId    int
	Status    taskStatus
	StartTime int64
}

type ReduceTaskInfo struct {
	Status    taskStatus
	StartTime int64
}

type Coordinator struct {
	// Your definitions here.
	NReduce     int // the number of reduce tasks to use.
	MapTasks    map[string]*MapTaskInfo
	mu          sync.Mutex
	ReduceTasks []*ReduceTaskInfo
}

func (c *Coordinator) initTask(files []string) {
	for idx, fileName := range files {
		c.MapTasks[fileName] = &MapTaskInfo{
			TaskId: idx,
			Status: idle,
		}
	}
	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTaskInfo{
			Status: idle,
		}
	}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		return BadMsgType
	}
	// 选择一个任务返回给worker
	c.mu.Lock()
	defer c.mu.Unlock()

	count_map_success := 0
	for fileName, taskinfo := range c.MapTasks {
		alloc := false

		if taskinfo.Status == idle || taskinfo.Status == failed {
			// 选择闲置或者失败的任务
			alloc = true
		} else if taskinfo.Status == running {
			// 判断其是否超时, 超时则重新派发
			curTime := time.Now().Unix()
			if curTime-taskinfo.StartTime > 10 {
				taskinfo.StartTime = curTime
				alloc = true
			}
		} else {
			count_map_success++
		}

		if alloc {
			// 将未分配的任务和已经失败的任务分配给这个worker
			reply.MsgType = MapTaskAlloc
			reply.TaskName = fileName
			reply.NReduce = c.NReduce
			reply.TaskID = taskinfo.TaskId

			// log.Printf("coordinator: apply Map Task: taskID = %v\n", reply.TaskID)

			// 修改状态信息
			taskinfo.Status = running
			taskinfo.StartTime = time.Now().Unix()
			return nil
		}
	}

	if count_map_success < len(c.MapTasks) {
		// map任务没有可以分配的, 但都还未完成
		reply.MsgType = Wait
		return nil
	}

	count_reduce_success := 0
	// 运行到这里说明map任务都已经完成
	for idx, taskinfo := range c.ReduceTasks {
		alloc := false
		if taskinfo.Status == idle || taskinfo.Status == failed {
			alloc = true
		} else if taskinfo.Status == running {
			// 判断其是否超时, 超时则重新派发
			curTime := time.Now().Unix()
			if curTime-taskinfo.StartTime > 10 {
				taskinfo.StartTime = curTime
				alloc = true
			}
		} else {
			count_reduce_success++
		}

		if alloc {
			// 分配给其一个Reduce任务
			reply.MsgType = ReduceTaskAlloc
			reply.TaskID = idx

			// log.Printf("coordinator: apply Reduce Task: taskID = %v\n", reply.TaskID)

			taskinfo.Status = running
			taskinfo.StartTime = time.Now().Unix()
			return nil
		}
	}

	if count_reduce_success < len(c.MapTasks) {
		// reduce任务没有可以分配的, 但都还未完成
		reply.MsgType = Wait
		return nil
	}

	// 运行到这里说明所有任务都已经完成
	reply.MsgType = Shutdown

	return nil
}

// 更新任务状态
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if req.MsgType == MapSuccess {
		for _, v := range c.MapTasks {
			if v.TaskId == req.TaskID {
				v.Status = finished
				// log.Printf("coordinator: map task%v finished\n", v.TaskId)
				break
			}
		}
	} else if req.MsgType == ReduceSuccess {
		c.ReduceTasks[req.TaskID].Status = finished
		// log.Printf("coordinator: reduce task%v finished\n", req.TaskID)
	} else if req.MsgType == MapFailed {
		for _, v := range c.MapTasks {
			if v.TaskId == req.TaskID {
				v.Status = failed
				// log.Printf("coordinator: map task%v failed\n", v.TaskId)
				break
			}
		}
	} else if req.MsgType == ReduceFailed {
		c.ReduceTasks[req.TaskID].Status = failed
		// log.Printf("coordinator: reduce task%v failed\n", req.TaskID)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	// 先确认mapTask完成
	for _, taskinfo := range c.MapTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	// fmt.Println("Coordinator: All map task finished")

	// 再确认Reduce Task 完成
	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	// fmt.Println("Coordinator: All reduce task finished")

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		MapTasks:    make(map[string]*MapTaskInfo),
		ReduceTasks: make([]*ReduceTaskInfo, nReduce),
	}

	// Your code here.
	// 由于每一个文件名就是一个task ,需要初始化任务状态
	c.initTask(files)

	c.server()
	return &c
}
