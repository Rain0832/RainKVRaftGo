package mr

//
// rpc.go: RPC definitions.
//

import (
	"os"
	"strconv"
)

// 1. worker request task from coordinator
type RequestTaskArgs struct {
}

// 2. coordinator reply task to worker
type ReplyTaskArgs struct {
	TaskType string
	FileName string
	MapID    int
	ReduceID int
	NMap     int
	NReduce  int
}

// 3. worker report task complete status to coordinator
type ReportTaskArgs struct {
	TaskType string
	TaskID   int
}

// 4. coordinator reply task complete status to worker
type ReportTaskReplyArgs struct {
	OK bool
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
