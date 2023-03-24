package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestWorkArgs struct{}

type RequestWorkReply struct {
	IsMap     bool
	NReduce   int
	NMap      int
	TaskId    int // carry the TaskId when submit a task
	TaskIndex int // map task or a reduce task

	Filename string // for map task

}

// NOTE: we can merge "request/submit" as request, which means, request new task with the last submitted
// but for simplicity, sperate it as two rpc
type SubmitWorkArgs struct {
	TaskId int
}

type SubmitWorkReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
