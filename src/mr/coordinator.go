package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskTracker struct { 
	start        time.Time
	Redistribute int
	status       int // 0: not start; 1: in progress; 2: done
}

type Coordinator struct {
	files        []string
	taskTrackers []TaskTracker
	nMap         int
	nReduce      int
	phase        int // 0: map; 1: reduce; 2: done and exit
}

// RPC handlers for the worker to call.
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AllocateTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//fmt.Printf("%+v\n", c)
	for {
		if c.phase == 0 {
			for i := 0; i < c.nMap; i++ {
				
				if c.taskTrackers[i].status == 0 {
					fmt.Printf("Task %d: %+v\n", i, c.taskTrackers[i])
					reply.wait = false
					reply.Task = new(Task)
					reply.Task.TType = Map
					reply.Task.TaskId = i
					reply.Task.NReduce = c.nReduce
					reply.Task.NMap = c.nMap
					reply.Task.FileName = c.files[i]
					c.taskTrackers[i].start = time.Now()
					c.taskTrackers[i].status = 1
					fmt.Printf("allocate map task %d \n", reply.Task.TaskId) 
					fmt.Printf("Task detail: %+v\n", reply.Task)
					return nil
				}
			}
			//check is all done, some form of heart beat
			allDone := true
			for i := 0; i < c.nMap; i++ {
				if c.taskTrackers[i].status != 2 && time.Now().Sub(c.taskTrackers[i].start) > 10 * time.Duration(c.taskTrackers[i].Redistribute)*time.Second {
					fmt.Printf("Redistribute Task %d", i)
					c.taskTrackers[i].Redistribute++
					c.taskTrackers[i].start = time.Now()
					c.taskTrackers[i].status = 0
					continue;
				} 
				allDone = allDone && (c.taskTrackers[i].status == 2)
			}
			if !allDone {
				reply.wait = true
				return nil
			}

			//if all done, then change phase to 1
			c.phase = 1
			c.taskTrackers = make([]TaskTracker, c.nReduce)
			fmt.Printf("Finish Map phase, go to reduce phase\n")
			continue;
		} else if c.phase == 1 {
			for j := 0; j < c.nReduce; j++ {
				if c.taskTrackers[j].status == 0 {
					fmt.Printf("Task %d: %+v\n", j, c.taskTrackers[j])
					reply.wait = false
					reply.Task = new(Task)
					reply.Task.TType = Reduce
					reply.Task.TaskId = j
					reply.Task.NReduce = c.nReduce
					reply.Task.NMap = c.nMap
					reply.Task.FileName = ""
					c.taskTrackers[j].start = time.Now()
					c.taskTrackers[j].status = 1
					fmt.Printf("allocate reduce task %d \n", reply.Task.TaskId) 
					fmt.Printf("Task detail: %+v\n", reply.Task)
					return nil
				} 
			}
			//check is all done, some form of heart beat
			//if all done, then change phase to 2
			//check is all done, some form of heart beat
			allDone := true
			for i := 0; i < c.nReduce; i++ {
				if c.taskTrackers[i].status != 2 && time.Now().Sub(c.taskTrackers[i].start) > 10*time.Duration(c.taskTrackers[i].Redistribute)*time.Second {
					fmt.Printf("Redistribute Task %d \n", i)
					fmt.Printf("%+v\n", c)
					c.taskTrackers[i].Redistribute++
					c.taskTrackers[i].start = time.Now()
					c.taskTrackers[i].status = 0
					continue;
				}
				allDone = allDone && (c.taskTrackers[i].status == 2)
			}
			if !allDone {
				reply.wait = true
				return nil
			}
			c.phase = 2 
		}
		reply.wait = true
		return nil
	}
}

// a call to Done means that the worker has finished processing
func (c *Coordinator) DoneTask(doneArgs *DoneArgs, doneReply *DoneReply) error {
	if doneArgs.TType == Map && c.phase == 0 {
		c.taskTrackers[doneArgs.TaskId].status = 2
	} else if doneArgs.TType == Reduce && c.phase == 1 {
		c.taskTrackers[doneArgs.TaskId].status = 2
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
	ret := false

	// Your code here.
	if c.phase == 2 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = 0
	// Your code here.
	c.files = files
	if c.nMap > c.nReduce {
		c.taskTrackers = make([]TaskTracker, c.nMap)
	} else {
		c.taskTrackers = make([]TaskTracker, c.nReduce)
	}
	c.server()
	return &c
}

//TODO: Should keep track all the tasks to corresponding workers socket, as well as it status (it might be done if no associated worker)
