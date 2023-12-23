package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

type MsgType int

var (
	BadMsgType = errors.New("bad message type")
	NoMoreTask = errors.New("no more task left")
)

const (
	AskForTask      MsgType = iota // `Worker`请求任务
	MapTaskAlloc                   // `Coordinator`分配`Map`任务
	ReduceTaskAlloc                // `Coordinator`分配`Reduce`任务
	MapSuccess                     // `Worker`报告`Map`任务的执行成功
	MapFailed                      // `Worker`报告`Map`任务的执行失败
	ReduceSuccess                  // `Worker`报告`Reduce`任务的执行成功
	ReduceFailed                   //`Worker`报告`Reduce`任务的执行失败
	Shutdown                       // `Coordinator`告知`Worker`退出（所有任务执行成功）
	Wait                           //`Coordinator`告知`Worker`休眠（暂时没有任务需要执行）
)

type MessageSend struct {
	MsgType MsgType
	TaskID  int // `Worker`回复的消息类型如MapSuccess等需要使用
}

type MessageReply struct {
	MsgType  MsgType
	NReduce  int    // MapTaskAlloc需要告诉Map Task 切分的数量
	TaskID   int    // 任务Id用于选取输入文件
	TaskName string // MapSuccess专用: 告知输入.txt文件的名字
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
