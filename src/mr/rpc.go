package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type TaskType int

const (
	EXIT TaskType = iota
	WAIT
	MAP
	REDUCE
)

type Status int

const (
	UNASSIGN Status = iota
	ASSIGN
	FINISH
)

// Add your RPC definitions here.
type MRTask struct {
	TaskType    TaskType
	Status      Status
	Timestamp   time.Time
	Index       int
	InputFiles  []string
	OutputFiles []string
}

type RequestTaskReply struct {
	TaskNo  int
	Task    MRTask
	NReduce int
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
