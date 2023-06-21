package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"



// Add your RPC definitions here.
type RequestTaskArgs struct {}

type RequestTaskReply struct {
	Task *Task
	wait bool // if true, then wait for a while and then ask again

}

type DoneArgs struct {
	TaskId int // if map task, the it mean finish create all intermediate files, if reduce task, it mean finish create the final output file.
}

type DoneReply struct {
}

//common data strucutre
//should declare in a lib.go file
//declare here for convenience
type TaskType int
const (
	Map TaskType = 0
	Reduce TaskType = 1
	Done TaskType = 2
)

type Task struct {
	TType int // 0: map; 1: reduce
	TaskId int // map or reduce task id
	FileName string // map task: file name; reduce task: intermediate file name
	NReduce int // number of reduce tasks = total number of intermediate files
	NMap int // number of map tasks = total number of files to process
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
