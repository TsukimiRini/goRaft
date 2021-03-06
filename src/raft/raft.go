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
	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

const (
	follower  = 0
	candidate = 1
	leader    = 2
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
	Term    int
	Command interface{}
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

	commitIndex int
	lastApplied int

	// ???????????????leader?????????
	nextIndex  []int // ?????????server????????????????????????log???
	matchIndex []int // ?????????server??????????????????log???

	timeout int
	status  int

	applyCh chan ApplyMsg
}

func (rf *Raft) GetLastLogIdx() int {
	return len(rf.logs) - 1
}

func (rf *Raft) GetLastLogTerm() int {
	if rf.GetLastLogIdx() < 0 {
		return -1
	}
	return rf.logs[rf.GetLastLogIdx()].Term
}

func (rf *Raft) GetPrevLogIdx(idx int) int {
	return rf.nextIndex[idx] - 1
}

func (rf *Raft) GetPrevLogTerm(idx int) int {
	if rf.GetPrevLogIdx(idx) < 0 {
		return -1
	}
	return rf.logs[rf.GetPrevLogIdx(idx)].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.status == leader)
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) ApplyMsgInOneRound() {
	DPrintf("ApplyMsg #%v: last applied %v, commitIndex %v", rf.me, rf.lastApplied, rf.commitIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{true, rf.logs[rf.lastApplied].Command, rf.lastApplied}
		rf.applyCh <- msg
	}
}

func (rf *Raft) LeaderUpdateCommitIdx(server int) {
	idx := rf.matchIndex[server]
	if idx > rf.commitIndex {
		cnt := 0
		for _, matched := range rf.matchIndex {
			if matched >= idx {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 && rf.logs[idx].Term == rf.currentTerm {
			DPrintf("LeaderUpdateCommitIdx #%v: commitIndex updated from %v to %v", rf.me, rf.commitIndex, idx)
			rf.commitIndex = idx
			rf.ApplyMsgInOneRound()
		} else {
			DPrintf("LeaderUpdateCommitIdx #%v: commitIndex not updated because cnt=%v", rf.me, cnt)
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	DPrintf("persist #%v: current%v, voterFor%v", rf.me, rf.currentTerm, rf.votedFor)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("read persistent data error")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		DPrintf("readPersist #%v: current%v, voterFor%v", rf.me, rf.currentTerm, rf.votedFor)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

	LastLogIndex int
	LastLogTerm  int
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

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply struct
//
type AppendEntriesReply struct {
	Term    int
	Success bool

	InconsistentTerm  int
	InconsistentIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RequestVote #%v: term of %v too old", rf.me, args.CandidateId)
		return
	}
	if rf.currentTerm < args.Term {
		rf.beFollower(args.Term)
	}
	if rf.GetLastLogTerm() > args.LastLogTerm || rf.GetLastLogTerm() == args.LastLogTerm && rf.GetLastLogIdx() > args.LastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RequestVote #%v: log of %v too old, term %v idx %v VS term %v idx %v",
			rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.GetLastLogTerm(), rf.GetLastLogIdx())
		return
	}
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.persist()
		DPrintf("RequestVote #%v: admit %v", rf.me, args.CandidateId)
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("RequestVote #%v: have voted for %v", rf.me, rf.votedFor)
	}
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.InconsistentIndex = -1
	reply.InconsistentTerm = -1
	if args.Term < rf.currentTerm {
		DPrintf("AppendEntries #%v: term of %v too old", rf.me, args.LeaderId)
		return
	}
	if rf.GetLastLogIdx() < args.PrevLogIndex {
		reply.InconsistentIndex = rf.GetLastLogIdx()
		DPrintf("AppendEntries #%v: log inconsistency, no entry in idx %v by %v",
			rf.me, args.PrevLogIndex, args.LeaderId)
		return
	} else if args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("rf.GetLastLogIdx() %v args.PrevLogIndex %v", rf.GetLastLogIdx(), args.PrevLogIndex)
		inconsistentTerm := rf.logs[args.PrevLogIndex].Term
		reply.InconsistentTerm = inconsistentTerm

		for i := 0; i < len(rf.logs); i++ {
			if rf.logs[i].Term == inconsistentTerm {
				reply.InconsistentIndex = i
				break
			}
		}

		rf.logs = rf.logs[:args.PrevLogIndex-1]
		rf.persist()
		DPrintf("AppendEntries #%v: log inconsistency, Term %v not %v by %v in idx %v",
			rf.me, reply.InconsistentTerm, args.PrevLogTerm, args.PrevLogIndex, args.LeaderId)
		return
	}
	rf.beFollower(args.Term)

	// var argsEn []string = []string{"args.Entries"}
	// for _, ent := range args.Entries {
	// 	argsEn = append(argsEn, strconv.Itoa(ent.Term))
	// }
	// if len(args.Entries) > 0 {
	// 	DPrintf(strings.Join(argsEn, " "))
	// 	DPrintf("%s", args.Entries[0].Command)
	// }
	pointer := args.PrevLogIndex
	for i, log := range args.Entries {
		pointer++
		if pointer > rf.GetLastLogIdx() {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
		if log.Term == rf.logs[pointer].Term {
			continue
		} else {
			rf.logs = rf.logs[:pointer]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	// reply.Term = rf.currentTerm
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.GetLastLogIdx())))
		rf.ApplyMsgInOneRound()
	}
	DPrintf("AppendEntries #%v: appendEntries from %v in term %v", rf.me, args.LeaderId, reply.Term)
	// var str []string = []string{"rf.logs", strconv.Itoa(rf.GetLastLogIdx())}
	// for _, ent := range rf.logs {
	// 	str = append(str, strconv.Itoa(ent.Term))
	// }
	// DPrintf(strings.Join(str, " "))
	// if rf.GetLastLogIdx() > 0 {
	// 	DPrintf("command: %s", rf.logs[rf.GetLastLogIdx()].Command)
	// }
}

func GetInitTimeout() int {
	return rand.Intn(200) + 300
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.status == leader)

	if !isLeader {
		return index, term, isLeader
	}

	DPrintf("Start #%v: command %s", rf.me, command)

	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()
	index = rf.GetLastLogIdx()
	rf.matchIndex[rf.me] = index

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

func (rf *Raft) leaderSendHeartBeats() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(idx int) {
				for {
					rf.mu.Lock()
					args := &AppendEntriesArgs{
						rf.currentTerm,
						rf.me, rf.GetPrevLogIdx(idx),
						rf.GetPrevLogTerm(idx),
						append(make([]LogEntry, 0), rf.logs[rf.nextIndex[idx]:]...),
						rf.commitIndex}
					rf.mu.Unlock()
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntires(idx, args, reply)
					// if !rf.sendAppendEntires(i, args, reply) {
					// 	return false, term
					// }
					rf.mu.Lock()
					if !ok || rf.status != leader || rf.currentTerm != args.Term {
						rf.mu.Unlock()
						return
					} else if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[idx] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[idx] = rf.matchIndex[idx] + 1
						rf.LeaderUpdateCommitIdx(idx)
						rf.mu.Unlock()
						return
					} else {
						rf.nextIndex[idx] = reply.InconsistentIndex
						rf.mu.Unlock()
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) beLeader() {
	if rf.status != leader {
		DPrintf("%v converted to leader", rf.me)
	}
	rf.votedFor = rf.me
	rf.persist()
	rf.status = leader // leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.logs)
	}
}

func (rf *Raft) beFollower(term int) {
	if rf.status != follower {
		DPrintf("%v converted to follower", rf.me)
	}
	rf.currentTerm = term
	rf.status = follower
	rf.votedFor = -1
	rf.timeout = GetInitTimeout()
	rf.persist()
}

func (rf *Raft) beCandidate() {
	if rf.status != candidate {
		DPrintf("%v converted to candidate", rf.me)
	}
	rf.status = candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
	rf.timeout = GetInitTimeout()
}

func (rf *Raft) kickOffElection() {
	var votedCnt int32 = 1
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(i int) {
				rf.mu.Lock()
				term := rf.currentTerm
				rf.mu.Unlock()
				DPrintf("%v try to get admitted by %v in term %v", rf.me, i, term)
				args := &RequestVoteArgs{term, rf.me, rf.GetLastLogIdx(), rf.GetLastLogTerm()}
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(i, args, reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.beFollower(reply.Term)
						return
					}
					if rf.status != candidate || rf.currentTerm != args.Term {
						return
					}
					if reply.VoteGranted {
						DPrintf("%v get admitted by %v", rf.me, i)
						atomic.AddInt32(&votedCnt, 1)
						if atomic.LoadInt32(&votedCnt) > int32(len(rf.peers)/2) {
							rf.beLeader()
						}
					}
				} else {
					DPrintf("%v not get package from %v in term %v", rf.me, i, term)
				}
			}(idx)
		}
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.timeout = GetInitTimeout()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.status = 0 // follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.logs = make([]LogEntry, 1)

	rf.applyCh = applyCh

	// follower
	go func() {
		for !rf.killed() {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.status == follower || rf.status == candidate {
				rf.timeout -= 10
			}
			if (rf.status == follower || rf.status == candidate) && rf.timeout <= 0 {
				DPrintf("%v try to be leader", rf.me)
				rf.beCandidate()
				rf.mu.Unlock()
				rf.kickOffElection()
			} else {
				rf.mu.Unlock()
			}
		}
	}()

	// leader
	go func() {
		for !rf.killed() {
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			if rf.status == leader {
				rf.mu.Unlock()
				rf.leaderSendHeartBeats()
			} else {
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
