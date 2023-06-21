package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//

type KeyValue struct {
	Key   string
	Value string
}

type Worker struct {
	Mapf func(string, string) []KeyValue
	Reducef func(string, []string) string
	Task *Task
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
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
	w := new (Worker)
	w.Mapf = mapf
	w.Reducef = reducef
	w.RequestTask()
}


func (w *Worker)RequestTask() {
	requestTaskArgs := RequestTaskArgs{}
	requestTaskReply := RequestTaskReply{}
	endSignal := false
	for !endSignal {
		ok := call("Coordinator.AllocateTask", &requestTaskArgs, &requestTaskReply)
		w.Task = requestTaskReply.Task
		if ok {
			if requestTaskReply.Wait {
				sleep(1000)
			} else {
				if requestTaskReply.Task.TType == Map {
					w.doMapTask()
				} else if requestTaskReply.Task.TType == Reduce {
					w.doReduceTask()
				} else if requestTaskReply.Task.TType == Done {
					endsignal = true
				} 
			}
		} else {
			fmt.Printf("call failed!\n , coordinator not responding")
		} 
	} 
}

func (w *Worker)doMapTask() {  

	intermediate := [w.Task.nReduce][]mr.KeyValue{} 
	file, err := os.Open(w.Task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		kvIndex := ihash(kv.Key) % w.Task.nReduce
		intermediate[kvIndex] = append(intermediate[kvIndex], kv)
	}

	for i, inter := range intermediate {
		//write into intermediate file
		ifile,_ := os.create("mr-%d-%d",w,Task.TaskId,i)
		enc := json.NewEncoder(ifile)
		for _, kb := range inter {
			err := enc.Encode(&kb)
		}
		ifile.Close()
	} 
	doneTaskArgs := DoneArgs{TaskId:w.Task.TaskId}
	doneTaskReply := DoneReply{}
	ok := call("Coordinator.DoneTask", &doneTaskArgs, &doneTaskReply)
	if ok {
		fmt.Printf("done map task success!\n")
	} else {
		fmt.Printf("call failed!\n , coordinator not responding")
	}
}

func (w *Worker) doReduceTask() {
	var kva []KeyValue 
	for i := 0; i < w.Task.NMap; i++ {
		ifile, err := os.Open("mr-%d-%d",i,w.Task.TaskId)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//sorting
	sort.Sort(ByKey(intermediate))
	//create output file
	ofile, _ := os.Create("mr-out-%d",w.Task.TaskId)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close() 
	doneTaskArgs := DoneArgs{TaskId:w.Task.TaskId}
	doneTaskReply := DoneReply{}
	ok := call("Coordinator.DoneTask", &doneTaskArgs, &doneTaskReply)
	if ok {
		fmt.Printf("done map task success!\n")
	} else {
		fmt.Printf("call failed!\n , coordinator not responding")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
