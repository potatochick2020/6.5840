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

1. distributing map task/ finish distributing map task, but not all map task finish which means not all intermediates files being create. (If worker dies in this time, coordinator will re-distribute map task again) , a worker could enter sleep mode and request again after a period of time, both should work well.
2. distributing reduce task, finish distribute reduce task and not all reduce task finished 
3. done

# Lab 2:
## 2A: Leader Election

A Raft distributed system can tolerate up to n/2 faults, where n is the number of Raft instances in the system. For example, a Raft system with 5 instances can tolerate up to 2 instances experiencing faults such as network delays or loss of connectivity.

It might be a bit abstract to understand above statement, but it could simply be explained that, as long as there are still n/2+1 Worker working, a leader could still be selected and therefore the system could still be working.

### Design
In request vote of candidate stage, the candidate will send an request vote rpc to all other peers(machine in the same raft system), it is a must to put this into a go rouine/ start a new non-blocking thread, as it is not guarantine the working will reply, and it will lead to a timeout.

[Go routine with a stop signal](https://yourbasic.org/golang/stop-goroutine/#:~:text=One%20goroutine%20can't%20forcibly,suitable%20points%20in%20your%20goroutine.&text=Here%20is%20a%20more%20complete,for%20both%20data%20and%20signalling.)

