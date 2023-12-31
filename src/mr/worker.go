package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	Mapf    func(string, string) []KeyValue
	Reducef func(string, []string) string
	Task    *Task
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := new(worker)
	w.Mapf = mapf
	w.Reducef = reducef
	w.RequestTask()
	//fmt.printf("worker exit\n")
}

func (w *worker) RequestTask() {  
	for {
		//fmt.Printf("request task")
		requestTaskArgs := RequestTaskArgs{}
		requestTaskReply := RequestTaskReply{}
		//fmt.Printf("call rpc allocate task\n")
		ok := call("Coordinator.AllocateTask", &requestTaskArgs, &requestTaskReply)
		
		if ok {
			//fmt.Printf("received reply %+v \n", requestTaskReply)
			if requestTaskReply.Done {
				break
			} else {
				if requestTaskReply.Wait {
					time.Sleep(5 * time.Second)
				} else { 
					w.Task = requestTaskReply.Task
					if requestTaskReply.Task.TType == Map {
						//fmt.Printf("Do map task %d : %+v - Task: %+v \n", w.Task.TaskId,requestTaskReply,w.Task)
						w.doMapTask()
					} else if requestTaskReply.Task.TType == Reduce {
						//fmt.Printf("Do reduce task %d : %+v - Task: %+v \n", w.Task.TaskId,requestTaskReply,w.Task)
						w.doReduceTask()
					}
				}
			}
			
		} else {
			//fmt.printf("call failed!\n , coordinator not responding")
		}
	}
}

func (w *worker) doMapTask() {
	//fmt.printf("received map task %d \n", w.Task.TaskId)
	//fmt.printf("Task detail: %+v\n", w.Task)
	intermediate := make([][]KeyValue, w.Task.NReduce)
	file, err := os.Open(w.Task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	kva := w.Mapf(w.Task.FileName, string(content))
	for _, kv := range kva {
		kvIndex := ihash(kv.Key) % w.Task.NReduce
		intermediate[kvIndex] = append(intermediate[kvIndex], kv)
	}

	for i, inter := range intermediate {
		//write into intermediate file
		ifilename := fmt.Sprintf("mr-%d-%d", w.Task.TaskId, i)
		ifile, _ := os.Create(ifilename)
		enc := json.NewEncoder(ifile)
		for _, kb := range inter {
			enc.Encode(&kb)
		}
		ifile.Close()
	}
	doneTaskArgs := DoneArgs{TType:w.Task.TType, TaskId: w.Task.TaskId}
	doneTaskReply := DoneReply{}
	ok := call("Coordinator.DoneTask", &doneTaskArgs, &doneTaskReply)
	if ok { 
		//fmt.printftf("Send done map task %d\n", w.Task.TaskId)
	} else {
		//fmt.printf("call failed!\n , coordinator not responding")
	}
}

func (w *worker) doReduceTask() {
	//fmt.printf("received reduce task %d \n", w.Task.TaskId)
	//fmt.printf("Task detail: %+v\n", w.Task)
	var kva []KeyValue
	for i := 0; i < w.Task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, w.Task.TaskId)
		ifile, _ := os.Open(filename)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//sorting
	sort.Sort(ByKey(kva))
	//create output file
	ofilename := fmt.Sprintf("mr-out-%d", w.Task.TaskId)
	ofile, _ := os.Create(ofilename)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := w.Reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
	doneTaskArgs := DoneArgs{TType:w.Task.TType, TaskId: w.Task.TaskId}
	doneTaskReply := DoneReply{}
	ok := call("Coordinator.DoneTask", &doneTaskArgs, &doneTaskReply)
	if ok {
		//fmt.printf("Send done reduce task %d\n", w.Task.TaskId)
	} else {
		//fmt.printf("call failed!\n , coordinator not responding")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
