package raft

import (
	"math/rand"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"github.com/sasha-s/go-deadlock"
)

const (
	FOLLOWER  int = 0
	CANDIDATE int = 1
	LEADER    int = 2
)

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state
	peersMu   []deadlock.Mutex    // Lock to protect shared access to other peer's state
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
	return time.Duration(50+(rand.Int63()%300)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term = rf.currentTerm
	var isleader = rf.role == LEADER
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
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	//2A, 2B
	DPrintf2A("Server %d : Received Requested Vote args %+v from Server %d\n", rf.me, args, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && !rf.voted) {
		//TODO: 2B add criteria that does not vote to leader
		//TODO: add if candidate’s log is at least as up-to-date as receiver’s log, grant vote
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		rf.voted, rf.votedFor, rf.currentTerm = true, args.CandidateId, args.Term
		rf.electionTimeOut.Reset(RamdomizedElection())
		DPrintf2A("Server %d : Voted to Server %d\n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted, reply.Term = false, rf.currentTerm
		reply.VoteGranted = false
		DPrintf2A("Server %d : Did Not Voted to Server %d\n", rf.me, args.CandidateId)
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeOut.Reset(RamdomizedElection())
	if len(args.Entries) > 0 {
		DPrintf2B("Server %d : Get AppendEntries RPC - args = %+v ", rf.me, args)
	}
	//1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	//TODO:
	//3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	//4. Append any new entries not already in the log
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry

	rf.role = FOLLOWER
	//Commit uncommitted log following leaders commit index
	for i := rf.commitIndex + 1; i <= min(rf.lastApplied, args.LeaderCommit); i++ {
		DPrintf2B("Server %d : As Follower, Committing log entry[%d] %+v\n", rf.me, i, rf.logs[i])
		rf.applyChannel <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
		rf.commitIndex++
	}

	//Add to log
	if len(args.Entries) > 0 {
		DPrintf2B("Server %d : Entries = %+v,  len(args.Entries) - (args.PrevLogIndex - rf.lastApplied + 1) %d", rf.me, args.Entries, len(args.Entries)-(args.PrevLogIndex-rf.lastApplied+1))
		for i := rf.lastApplied - args.PrevLogIndex; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Command: args.Entries[i]})
			rf.lastApplied++
		}
	}
	if len(args.Entries) > 0 {
		DPrintf2B("Server %d : Reply AppendEntries RPC with success , current log size is %+v", rf.me, len(rf.logs))
	}
	reply.Term, reply.Success = rf.currentTerm, true
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

	rf.logs = append(rf.logs, Log{Term: rf.currentTerm, Command: command})
	rf.lastApplied = rf.lastApplied + 1
	//Committed := false
	c := make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(counter int, self int, matchIndex []int, nextIndex []int, lastAppliedindex int) {
				//AppendEntriesArgs
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: matchIndex[counter],
					PrevLogTerm:  rf.logs[matchIndex[counter]].Term,
					LeaderCommit: rf.commitIndex,
				}
				args.Entries = make([]interface{}, lastAppliedindex-matchIndex[counter])
				for j := 0; j < len(args.Entries); j++ {
					args.Entries[j] = rf.logs[matchIndex[counter]+j+1].Command
				}

				reply := AppendEntriesReply{}
				DPrintf2B("Server %d : Send AppendEntries RPC to Server %d - Len of entries%+v , Leader Match Index : %+v, Leader Next Index : %+v", self, counter, len(args.Entries), matchIndex, nextIndex)
				if rf.peers[counter].Call("Raft.AppendEntries", &args, &reply) {
					DPrintf2B("Server %d : Server %d replied %+v , lastAppliedIndex %d", self, counter, reply, lastAppliedindex)
					if reply.Success {
						rf.peersMu[counter].Lock()
						rf.nextIndex[counter] = lastAppliedindex + 1
						rf.matchIndex[counter] = lastAppliedindex
						rf.peersMu[counter].Unlock()
						c <- 1
					} else {
						c <- 2
					}
				} else {
					DPrintf2B("Server %d : Server %d DID NOT replied", self, counter)
					c <- 2
				}
			}(i, rf.me, rf.matchIndex, rf.nextIndex, rf.lastApplied)
		}
	}
	AppendEntriesSuccessCount := 1
	AppenEntriesFailCount := 0
	for {
		switch <-c {
		case 1:
			AppendEntriesSuccessCount++
		case 2:
			AppenEntriesFailCount++
		}
		if AppendEntriesSuccessCount > len(rf.peers)/2 {
			DPrintf2B("Server %d : Send Apply message, Command %+v, CommandIndex %d ", rf.me, command, rf.lastApplied)
			for i := rf.commitIndex + 1; i <= rf.lastApplied; i++ {
				rf.applyChannel <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
				rf.commitIndex++
			}
			break
		}
		if AppenEntriesFailCount > len(rf.peers)/2 {
			break
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == LEADER {
		DPrintf2B("Server %d : Send Append Entries RPC , Command %t", rf.me, command)
		rf.AppendNewEntry(command)
		return rf.lastApplied, rf.currentTerm, true
	}
	return -1, -1, false

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
			go func(counter int, matchIndex []int) {
				//TOTEST: Add prev log index and prev log term
				//TOTEST: this will affect appendentries RPC handler
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: matchIndex[counter],
					PrevLogTerm:  rf.logs[matchIndex[counter]].Term,
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				if rf.peers[counter].Call("Raft.AppendEntries", &args, &reply) {
					DPrintf2A("Server %d : Server %d received heartbeat", rf.me, counter)
				} else {
					DPrintf2A("Server %d : Server %d did not received heartbeat", rf.me, counter)
				}
			}(i, rf.matchIndex)
		}
	}
}

func (rf *Raft) StartElection() {
	DPrintf2A("Server %d : Start Election", rf.me)
	//request vote
	rf.currentTerm += 1
	voteReceived := 1
	rf.voted = true
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
				DPrintf2A("Server %d : Server %d received heartbeat", rf.me, counter)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.role == CANDIDATE {
					if reply.VoteGranted {
						voteReceived += 1
						if voteReceived >= len(rf.peers)/2+1 {
							DPrintfAll("Server %d : Received majority vote, change to leader", rf.me)
							rf.role = LEADER
							rf.heartbeatTimeOut.Reset(StandardHeartBeat())
							rf.nextIndex, rf.matchIndex = make([]int, len(rf.peers)), make([]int, len(rf.peers))
							for i := range rf.nextIndex {
								rf.nextIndex[i] = rf.lastApplied
							}
						}
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm, rf.role = reply.Term, FOLLOWER
					}
				}

			}
		}(i)
	}
}

// https://go.dev/tour/concurrency/5 : select statement for 2 time out : election time out and heartbeat time out
// https://pkg.go.dev/github.com/docker/swarmkit/manager/dispatcher/heartbeat

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.heartbeatTimeOut.C:
			rf.mu.Lock()
			if rf.role == LEADER {
				rf.SendHeartbeat()
				rf.heartbeatTimeOut.Reset(StandardHeartBeat())
			}
			rf.mu.Unlock()
		case <-rf.electionTimeOut.C:
			rf.mu.Lock()
			if rf.role != LEADER {
				//Enter Candidate State
				rf.role = CANDIDATE
				rf.StartElection()
				rf.electionTimeOut.Reset(RamdomizedElection())
			}
			rf.mu.Unlock()
		}
	}
	DPrintfKill("Server %d : Killed", rf.me)
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
		role:             FOLLOWER,
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
