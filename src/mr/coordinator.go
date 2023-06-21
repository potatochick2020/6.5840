package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

type TaskTracker struct {
	task id int 
	start Time
	Redistribute int 
	status int // 0: not start; 1: in progress; 2: done
}

type Coordinator struct {
	files []string
	taskTrackers []TaskTracker 
	nMap int
	nReduce int 
	phase int // 0: map; 1: reduce; 2: done and exit
}

// RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AllocateTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if c.phase == 0 {
		for i := 0; i < c.nMap; i++ {
			if c.status[i] == 0 {
				reply.Task = new(Task)
				reply.Task.TType = Map
				reply.Task.TaskId = i
				reply.Task.NReduce = c.nReduce
				reply.Task.NMap = c.nMap
				reply.Task.FileName = c.files[i]
				c.taskTrackers[i].start = time.Now()
				c.taskTrackers[i].status = 1
				return nil
			}
		}
		//check is all done, some form of heart beat
		for i := 0; i < c.nMap; i++ {
			if c.status[i] != 2 && time.Now().Sub(c.taskTrackers[i].start) > 10 * c.taskTrackers[i].Redistribute{
				c.taskTrackers[i].Redistribute++
				c.taskTrackers[i].status = 0
				return nil
			} else {
				reply.wait = true
			}
		}	
		//if all done, then change phase to 1
		c.phase = 1
		c.status = make([]int, c.nReduce)
		c.time = make([]Time, c.nReduce)
		c.sockets = make([]string, c.nReduce)
	} else if c.phase == 1 {
		for i := 0; i < c.nReduce; i++ {
			reply.Task = new(Task)
			reply.Task.TType = Reduce
			reply.Task.TaskId = i
			reply.Task.NReduce = c.nReduce
			reply.Task.NMap = c.nMap
			reply.Task.FileName = strconv.Itoa(i) 
			return nil
		}
		//check is all done, some form of heart beat
		//if all done, then change phase to 2
	} else if c.phase == 2 {

	}
	return nil
}

// a call to Done means that the worker has finished processing
func (c *Coordinator) Done(doneArgs *DoneArgs, doneReply *DoneReply) error {
	c.status[doneArgs.TaskId] = 2
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.phase == done {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = mapPhase
	// Your code here.
	c.files = files
	if c.nMap > c.nReduce {
		c.taskTrackers = make([]TaskTracker,c.nMap)
	} else {
		c.taskTrackers = make([]TaskTracker,c.nReduce)
	} 
	c.server()
	return &c
}

//TODO: Should keep track all the tasks to corresponding workers socket, as well as it status (it might be done if no associated worker)
