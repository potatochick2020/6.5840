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
import "github.com/sasha-s/go-deadlock"
import (
	//	"bytes"
	//"fmt"
	"math/rand"
//	"sync"
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

// Log entry
type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        deadlock.Mutex          // Lock to protect shared access to this peer's state
	peersMu   []deadlock.Mutex        // Lock to protect shared access to other peer's state
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
	electionTimeOut  *time.Timer
	heartbeatTimeOut *time.Timer
	//2B
	logs         []Log // as Index is start from 1 from figure 2
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
	applyChannel chan ApplyMsg
}

func StandardHeartBeat() time.Duration {
	return 30 * time.Millisecond
}

func RamdomizedElection() time.Duration {
	return time.Duration(60+(rand.Int63()%300)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isleader = rf.role == 2
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
	//fmt.Printf("Server %d : Received Requested Vote args %+v from Server %d\n", rf.me, args, args.CandidateId)
	// Your code here (2A, 2B).
	//TODO: add logic about vote 2B
	//2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		//("Server %d : Change current Term in RequestVote Args\n", rf.me)
		rf.voted = false
		return
	}

	if args.Term >= rf.currentTerm && rf.voted == false {
		//TODO: add if candidate’s log is at least as up-to-date as receiver’s log, grant vote
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//fmt.Printf("Server %d : Voted to Server %d\n", rf.me, args.CandidateId)
		rf.voted = true
		rf.votedFor = args.CandidateId
		rf.electionTimeOut.Reset(RamdomizedElection())
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//fmt.Printf("Server %d : Did Not Voted to Server %d\n", rf.me, args.CandidateId)
	}
}

type AppendEntriesArgs struct {
	Term         int           //leader's term
	LeaderId     int           //so follower can redirect clients
	PrevLogIndex int           //index of log entry immediately preceding new ones
	PrevLogTerm  int           //term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*TODO:*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeOut.Reset(RamdomizedElection())
	rf.role = 0 
	// Continue if this is not a heart beat message
	if len(args.Entries) > 0 {
		//1. Reply false if term < currentTerm (§5.1)
		// if args.Term < rf.currentTerm {
		// 	reply.Term = rf.currentTerm
		// 	reply.Success = false
		// 	return
		// }
		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		// if args.Term == rf.currentTerm && rf.lastApplied < args.PrevLogIndex {
		// 	reply.Term = rf.currentTerm
		// 	reply.Success = false
		// 	return
		// }
		//3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
		//4. Append any new entries not already in the log
		//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry

		//return success and commit to LOG 
		rf.logs = append(rf.logs,Log{Term: rf.currentTerm, Command: args.Entries[0]})
		rf.lastApplied++
		DPrintf("Server %d : Get AppendEntries RPC %t", rf.me, rf.logs[rf.lastApplied].Command)
		//TODO: Send an APPLY MSG to himself
	
		rf.applyChannel <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
	
		DPrintf("Server %d : Send Apply Message, return success %t", rf.me, rf.logs[rf.lastApplied].Command)
		 
		reply.Term, reply.Success = rf.currentTerm, true
		return
	}
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
func (rf *Raft) AppendNewEntry(command interface{}) {
	//TODO: Send append entries rpc
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastApplied,
		PrevLogTerm:  rf.logs[rf.lastApplied].Term,
		Entries:      []interface{}{command},
		LeaderCommit: rf.commitIndex,
	}
	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Command: command})
	rf.lastApplied = rf.lastApplied + 1
	
	DPrintf("Server %d : wg size %d", rf.me, len(rf.peers)-1)
	//Committed := false
	c := make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(counter int, self int) { 
				reply := AppendEntriesReply{}
				//fmt.Printf("Server %d : Send HeartBeat to Server %d \n", rf.me, counter)
				if rf.peers[counter].Call("Raft.AppendEntries", &args, &reply) { 
					defer DPrintf("Server %d : Server %d replied %+v", self, counter, reply) 
					if reply.Success == true {  
						c <- 1 
					} else {
						c <- 2
					}
					rf.peersMu[counter].Lock()
					rf.nextIndex[counter]++
					rf.matchIndex[counter]++  
					rf.peersMu[counter].Unlock()
				} 
			}(i, rf.me)
		}
	}
	AppendEntriesSuccessCount := 0
	AppenEntriesFailCount := 0
	for { 
		switch <- c {
		case 1 :
			AppendEntriesSuccessCount++
		case 2 :
			AppenEntriesFailCount++
		}
		if AppendEntriesSuccessCount >  len(rf.peers) / 2 { 
			applyMessage := ApplyMsg{CommandValid: true, Command: command, CommandIndex: rf.lastApplied}
			rf.applyChannel <- applyMessage
			break;
		}
		if AppendEntriesSuccessCount + AppenEntriesFailCount > len(rf.peers) {
			break;
		}
	}
	DPrintf("Server %d : Check Wait", rf.me)
	//TODO: IF majority of append entries reply success then commit 

	return
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock() 
	if rf.role == 2 {
		rf.AppendNewEntry(command)
		return rf.lastApplied, rf.currentTerm, true
	}
	return -1,-1, false

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
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(counter int) {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{} 
				if rf.peers[counter].Call("Raft.AppendEntries", &args, &reply) {
					DPrintf("Server %d : Server %d received heartbeat", rf.me, counter) 
				} else {
					DPrintf("Server %d : Server %d did not received heartbeat", rf.me, counter)
				}
			}(i)
		}
	}
}

func (rf *Raft) StartElection() {
	//request vote
	//fmt.Printf("Server %d : RequestVote \n", rf.me)
	//("Server %d : Change current Term as start election\n", rf.me)
	rf.currentTerm += 1
	voteReceived := 1
	rf.voted = true
	//Reset election time out
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
	/*TODO:
	lastLogIndex index of candidate’s last log entry (§5.4)
	lastLogTerm term of candidate’s last log entry (§5.4)
	*/
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(counter int) {
			reply := RequestVoteReply{}
			if rf.peers[counter].Call("Raft.RequestVote", &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.role == 1 {
					if reply.VoteGranted {
						voteReceived += 1
						if voteReceived >= len(rf.peers)/2+1 {
							rf.role = 2
							rf.heartbeatTimeOut.Reset(StandardHeartBeat())
							rf.nextIndex, rf.matchIndex = make([]int, rf.lastApplied), make([]int, 0)
						}
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm, rf.role = reply.Term, 0
					}
				}

			}
		}(i)
	}
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
				rf.heartbeatTimeOut.Reset(StandardHeartBeat())
			}
			rf.mu.Unlock()
		case <-rf.electionTimeOut.C:
			//fmt.Printf("Server %d : ElectionTimeOut\n", rf.me)
			rf.mu.Lock()
			//change to candidate
			if rf.role != 2 {
				rf.role = 1
				rf.StartElection()
				rf.electionTimeOut.Reset(RamdomizedElection())
			}
			rf.mu.Unlock()
		}
	}
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
		currentTerm:      0,
		role:             0,
		voted:            false,
		electionTimeOut:  time.NewTimer(RamdomizedElection()),
		heartbeatTimeOut: time.NewTimer(StandardHeartBeat()),
		//2B
		//TODO: add last index term,
		peersMu:     make([]deadlock.Mutex, len(peers)),
		logs:        make([]Log, 0),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		//Apply ch for sending apply msg
		applyChannel: applyCh,
	}
	//add initiate dummy log
	rf.logs = append(rf.logs, Log{Term: 0, Command: nil})

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
