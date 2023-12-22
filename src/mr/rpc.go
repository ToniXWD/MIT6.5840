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

const (
	AskForTask MsgType = iota
	MapTaskAlloc
	ReduceTaskAlloc
	MapSuccess
	MapFailed
	ReduceSuccess
	ReduceFailed
	Shutdown
	Wait
)

var (
	BadMsgType = errors.New("bad message type")
	NoMoreTask = errors.New("no more task left")
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MessageSend struct {
	MsgType MsgType
	TaskID  int
}

type MessageReply struct {
	MsgType  MsgType
	NReduce  int
	TaskID   int
	TaskName string
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
