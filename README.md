# 6.5840
My (potatochick2020) implementation of MIT 6.5840 (previous 6.824) Distributed System
This readme.md include the process of thinking.

# Lab 1: Map Reduce
The difficulty of this lab is understanding the intermediate files creation. 

1. Understand Responsibility of map and reduce function
```
map ("I am potato I am chick") -> {"I",1},{"am",1},{"potato","1"},{"I",1},{"am",1},{"chick",1}
reduce({"I",1},{"am",1},{"potato","1"},{"I",1},{"am",1},{"chick",1}) -> {"I",2},{"am",2},{"potato","1"},{"chick",1}
```

2. Understand intermediate files, 
> Periodically, the buffered pairs are written to local disk, partitioned into R regions by the partitioning function.
When I read this line, I thought there will be R intermediates files in total.
But actually this line refer to each map task create R intermediates files. 
> When a reduce worker has read all intermediate data, it sorts it by the intermediate keys so that all occurrences of the same key are grouped together
With my original understanding, this line means each worker nead to read *ALL* data and sort it. Which does not really make any sense. 
after figure out the actual meaning, I got a broad understand about the map reduce system.

3. Fault tolerance
keep a heart beat message on whether or not the worker still survive/ working on the task.

4. Worker Id? 
At this stage, I decide to start with not keep track workerId as worker could initiate connection by requesting task. So the master could simply keep track on task and the corresponding worker. In other words, instead of a system of registering worker, then allocate task to worker. I will have a system of total task and allocate a worker to it. 

5. Phase, order
The paper simply split phase to map, reduce phase. I wonder can I start reduce phase before I finish all map.
From chat gpt-4 from bing AI, it says it is possible and even give me a documentation from IBM. 

>In a MapReduce system, the reduce tasks can start before all map tasks have finished. There are 3 steps in the reduce phase: 1) copy (data to reducers), 2) sort (or more exactly merge), and 3) reduce (execution of reduce()). Reducers can start copying data from a map task as soon as that map task completes its execution 1.
> 
> By default, some schedulers wait until a certain percentage of the map tasks in a job have completed before scheduling reduce tasks for the same job.This percentage can be configured by setting the mapreduce.job.reduce.slowstart.completedmaps parameter to a value between 0 and 1 2.[IBM doc:](https://www.ibm.com/docs/en/spectrum-symphony/7.3.1?topic=tuning-reduce-tasks-started-based-map-tasks-finished)

I decided to go with the easy way for the coordinator to tell the worker which phase currently the system is

1. distributing map task
2. finish distributing map task, but not all map task finish which means not all intermediates files being create. (if worker dies in this time, coordinator will distribute map task again) , the idea is the master could keep a queue for all workers who had finish a task, or a worker could enter sleep mode and request again after a period of time, both should work well.
3. distributing reduce task
4. finish distribute reduce task
5. done

# Lab 2 : Raft
## 2A: Leader election
https://thesecretlivesofdata.com/raft/#election 

```
Test (2A): initial election ...
--- FAIL: TestInitialElection2A (4.89s)
    config.go:454: expected one leader, got none
Test (2A): election after network failure ...
--- FAIL: TestReElection2A (5.07s)
    config.go:454: expected one leader, got none
Test (2A): multiple elections ...
--- FAIL: TestManyElections2A (4.97s)
    config.go:454: expected one leader, got none
```

To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet), and have the leader send them out periodically. Write an AppendEntries RPC handler method that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.
