package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	currentTerm      int
	votedFor         int
	voted            bool
	role             int //0 for follower, 1 for Candidates, 2 for Leaders
	electionTimeOut  time.Timer
	heartbeatTimeOut time.Timer
}

func StandardHeartBeat() time.Duration {
	return 5 * time.Second
}

func RamdomizedElection() time.Duration {
	return time.Duration(50+(rand.Int63()%300)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == 2
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	//2A
	Term        int //candidate's term
	CandidateId int //candidate requesting vote
	//2B
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("Server %d : Received Requested Vote args %+v from Server %d\n", rf.me, args, args.CandidateId)
	// Your code here (2A, 2B).

	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//TODO: Refine Logic
	sameOrUpdate := args.Term >= rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voted = false
	}

	if sameOrUpdate && rf.voted == false {
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		fmt.Printf("Server %d : Voted to Server %d\n", rf.me, args.CandidateId)
		rf.voted = true
		rf.votedFor = args.CandidateId
		rf.electionTimeOut.Reset(RamdomizedElection())
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("Server %d : Did Not Voted to Server %d\n", rf.me, args.CandidateId)
	}
}

type AppendEntriesArgs struct {
}

type AppendEntriesReply struct {
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("Received Heartbeat \n")
	fmt.Printf("Server %d : Received Heartbeat  \n", rf.me)
	rf.mu.Lock()
	//TODO: make sure replace election timeout with timer
	rf.electionTimeOut.Reset(RamdomizedElection())
	rf.heartbeatTimeOut.Stop()
	rf.role = 0
	rf.mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a Lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SendHeartbeat() {
	//send append entries
	//TODO: make sure replace election timeout with timer
	rf.heartbeatTimeOut.Reset(StandardHeartBeat())
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(counter int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				fmt.Printf("Server %d : Send HeartBeat to Server %d \n", rf.me, i)
				ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				if !ok {
					fmt.Printf("Server %d : Server %d cannot receive heartbeat\n", rf.me, i)
				}
			}(i)

		}

	}
}

func (rf *Raft) SendRequestVote() {

}

func (rf *Raft) StartElection() {
	//request vote
	fmt.Printf("Server %d : RequestVote \n", rf.me)
	rf.currentTerm += 1
	voteReceived := 1
	//Reset election time out
	//TODO: make sure replace election timeout with timer
	rf.electionTimeOut.Reset(RamdomizedElection())
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(counter int) {

			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			reply := RequestVoteReply{}
			if rf.peers[counter].Call("Raft.RequestVote", &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Lock()
				//TODO: Race handling by breaking ?
				if reply.VoteGranted {
					voteReceived += 1
					if voteReceived >= len(rf.peers)/2+1 {
						fmt.Printf("Server %d : get elected \n", rf.me)
						rf.role = 2
						//do heart beat once
						rf.SendHeartbeat()

					}
				} else if reply.Term > rf.currentTerm {
					//goback to follower
					rf.currentTerm = reply.Term
					rf.role = 0
				}
			}
		}(i)
	}
	fmt.Printf("Server %d : Get %d vote in %d - need %d\n", rf.me, voteReceived, len(rf.peers), len(rf.peers)/2+1)
}

// https://go.dev/tour/concurrency/5 : select statement for 2 time out : election time out and heartbeat time out
// https://pkg.go.dev/github.com/docker/swarmkit/manager/dispatcher/heartbeat

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.heartbeatTimeOut.C:
			rf.mu.Lock()
			if rf.role == 2 {
				rf.SendHeartbeat()
			}
			rf.mu.Unlock()
		case <-rf.electionTimeOut.C:
			rf.mu.Lock()
			if rf.voted == false {

				//change to candidate
				rf.role = 1
				rf.StartElection()
			}
			rf.mu.Unlock()
		}
	}
	/*

		for rf.killed() == false {

			// Your code here (2A)
			// Check if a leader election should be started.
			// If the server is a follower and it is not following and leader then start leader election (Initial)
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			//TODO: election time out

			skipSleep:
			if rf.killed() {
				break;
			}
			rf.mu.Lock()
				if rf.role == 0 {
					rf.electionTimeoutMutex.Lock()
					if (rf.voted == false || time.Now().Sub(rf.electionTimeout) > time.Duration(5000) * time.Millisecond) {
						fmt.Printf("Server %d : Enter Candidate - rf.voted = %t , %s \n",rf.me, rf.voted, time.Now().Sub(rf.electionTimeout).String())
						fmt.Printf(time.Now().Sub(rf.electionTimeout).String())
						fmt.Printf((time.Duration(500)*time.Millisecond).String())
						fmt.Printf("\n")
						rf.electionTimeout = time.Now()
						//change to candidate
						rf.role = 1
						rf.mu.Unlock()
						rf.electionTimeoutMutex.Unlock()
						goto skipSleep;
					}
					rf.electionTimeoutMutex.Unlock()
				} else if rf.role == 1{
					rf.StartElection()

					goto skipSleep
					//if more then majority then become leader
					//role change to leader



				} else if rf.role == 2 {

				}
			rf.mu.Unlock()
		}
	*/
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		// Your initialization code here (2A, 2B, 2C).
		//2A
		currentTerm: 0,
		role:        0,
		voted:       false,
		//TODO: Add correct timer usage
		electionTimeOut:  time.newTimer(RamdomizedElection()),
		heartbeatTimeOut: time.newTimer(StandardHeartBeat()),
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start heart beat if it is
	return rf
}

//EXAMPLE CODE

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }
