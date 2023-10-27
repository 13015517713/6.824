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
type GetTaskArgs struct {
}
type GetTaskReply struct {
	RPC_nReduce   int
	RPC_files     []string
	RPC_task_id   int
	RPC_task_type int // 标识任务类型
}
type SubmitTaskArgs struct {
	RPC_task_type int // 标识任务类型
	RPC_task_id   int // for map task
	RPC_files     []string
	RPC_reduce_id []int
}
type SubmitTaskReply struct {
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
