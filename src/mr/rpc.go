package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type TaskRequestArgs struct {
	WorkerID int
}

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Complete
)

type TaskResponse struct {
	Type      TaskType // Map, Reduce, Wait, Complete
	FileName  string   // For map tasks
	TaskID    int      // ID of the task
	NumReduce int      // Number of reduce tasks
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
