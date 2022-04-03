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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// a struct to hold information about each log entry
//
type LogEntry struct {
	term    int
	command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        []LogEntry
	leaderId    int

	timeout int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.leaderId == rf.me)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	// lastLogIndex int
	// lastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments struct
//
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// prevLogIndex int
	// prevLogTerm  int

	// entries      []LogEntry
	// leaderCommit int
}

//
// AppendEntries RPC reply struct
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		DPrintf("RequestVote #%v: term of %v too old", rf.me, args.CandidateId)
		return
	}
	if rf.currentTerm < args.Term || rf.votedFor == args.Term || rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		rf.leaderId = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		DPrintf("RequestVote #%v: admit %v", rf.me, args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RequestVote #%v: have voted for %v", rf.me, rf.votedFor)
	}
	rf.mu.Unlock()
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		DPrintf("AppendEntries #%v: term of %v too old", rf.me, args.LeaderId)
		return
	}
	rf.timeout = GetInitTimeout()
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.mu.Unlock()
	DPrintf("AppendEntries #%v: heartbeat from %v in term %v", rf.me, args.LeaderId, reply.Term)
}

func GetInitTimeout() int {
	return rand.Intn(250) + 250
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntires(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) leaderSendHeartBeats() (bool, int) {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i != rf.me {
			args := &AppendEntriesArgs{term, rf.me}
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntires(i, args, reply) {
				return false, term
			}
			if !reply.Success {
				return false, reply.Term
			}
		}
	}
	return true, term
}

func (rf *Raft) beLeader() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.leaderId = rf.me
	rf.mu.Unlock()
	rf.leaderSendHeartBeats()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.timeout = GetInitTimeout()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.leaderId = -1

	// Your initialization code here (2A, 2B, 2C).
	// follower
	go func() {
		for true {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.votedFor != rf.me {
				rf.timeout -= 10
			}
			if rf.votedFor != rf.me && rf.timeout <= 0 {
				DPrintf("%v try to be leader", rf.me)
				rf.currentTerm++
				rf.leaderId = -1
				rf.votedFor = rf.me
				term := rf.currentTerm
				// rf.votedFor = rf.me
				votedCnt := 1
				rf.timeout = GetInitTimeout()
				rf.mu.Unlock()
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					args := &RequestVoteArgs{term, rf.me}
					reply := &RequestVoteReply{}
					DPrintf("%v try to get admitted by %v in term %v", rf.me, i, term)
					if rf.sendRequestVote(i, args, reply) && reply.VoteGranted == true {
						DPrintf("%v get admitted by %v", rf.me, i)
						votedCnt++
					} else {
						DPrintf("%v not get admitted by %v", rf.me, i)
					}
				}
				if votedCnt > len(rf.peers)/2 {
					DPrintf("%v be leader", rf.me)
					rf.beLeader()
				}
			} else {
				rf.mu.Unlock()
			}
		}
	}()

	// leader
	go func() {
		for true {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.leaderId == rf.me {
				rf.mu.Unlock()
				ok, newTerm := rf.leaderSendHeartBeats()
				rf.mu.Lock()
				if !ok {
					rf.currentTerm = newTerm
					rf.votedFor = -1
					rf.leaderId = -1
				}
			}
			rf.mu.Unlock()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
