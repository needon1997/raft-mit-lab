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
	"bytes"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"raft/labgob"
	"raft/labrpc"

	"github.com/google/uuid"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type State int

var logger *log.Logger

const (
	FOLLOWER State = iota
	CANDIDATE
	LEADER
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *Persister          // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	dead              int32               // set by Kill()
	applyChan         chan ApplyMsg
	currentTerm       int
	votedFor          int
	log               []LogEntry
	commitIndex       int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int
	lastHeartBeatTime time.Time
	state             State
	lastIncludedIndex int
	lastIncludedTerm  int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
type LogEntry struct {
	Term    int
	Command interface{}
}

func init() {
	file, _ := os.Create("./log")
	logger = log.New(file, "log", log.Lmicroseconds)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
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
	//logger.Printf("server：%v  persist: currentTerm:%v,  votedFor:%v, log:%v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	log := make([]LogEntry, 0)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&log) != nil {
		//logger.Printf("server：%v  persist: currentTerm:%v,  votedFor:%v, log:%v  result: fail", rf.me, currentTerm, voteFor, log)
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
	} else {
		//logger.Printf("server：%v  persist: currentTerm:%v,  votedFor:%v, log:%v  result: success", rf.me, currentTerm, voteFor, log)
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.lastIncludedTerm <= lastIncludedTerm && rf.lastIncludedIndex < lastIncludedIndex {
		if rf.lastIncludedIndex+len(rf.log) <= lastIncludedIndex {
			rf.log = make([]LogEntry, 0)
		} else {
			rf.log = rf.log[lastIncludedIndex-rf.lastIncludedIndex:]
		}
		//logger.Printf("server：%v snapshot: currentTerm:%v,index:%v  term:%v log:%v", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.log)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(rf.currentTerm)
		e.Encode(rf.votedFor)
		e.Encode(rf.log)
		data := w.Bytes()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		if rf.commitIndex < rf.lastIncludedIndex {
			rf.commitIndex = rf.lastIncludedIndex
		}
		return true
	} else {
		return false
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	//logger.Printf("Snapshot at server:%v, index:%v", rf.me, index)
	term := rf.log[index-rf.lastIncludedIndex-1].Term
	rf.mu.Unlock()
	rf.CondInstallSnapshot(term, index, snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	RequestId    string
	// Your data here (2A, 2B).
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
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Log          []LogEntry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	RequestId    string
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	FirstLogIndex int
	LastLogTerm   int
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//logger.Printf("InstallSnapshot from:%v, to %v, lastIndex:%v, lastTerm:%v", arg.LeaderId, rf.me, arg.LastIncludedIndex, arg.LastIncludedTerm)
	if arg.Term >= rf.currentTerm {
		snapshotMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      arg.Data,
			SnapshotIndex: arg.LastIncludedIndex,
			SnapshotTerm:  arg.LastIncludedTerm,
		}
		rf.lastHeartBeatTime = time.Now()
		rf.currentTerm = arg.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
		rf.state = FOLLOWER
		rf.applyChan <- snapshotMsg
	}
	reply.Term = rf.currentTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//check if the request vote term equal to current term
	//if the request vote term less or equal to current term reject the vote
	//if the request vote term greater than current term, vote for it
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil
		rf.state = FOLLOWER
		lastLogTerm := 0
		lastLogIndex := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
			lastLogIndex = len(rf.log) + rf.lastIncludedIndex
		} else {
			lastLogTerm = rf.lastIncludedTerm
			lastLogIndex = rf.lastIncludedIndex
		}
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			//logger.Printf("voterequest:id:%s, from:%v, to: %v, result: %v\n", args.RequestId, args.CandidateId, rf.me, true)
		} else {
			reply.Term = args.Term
			reply.VoteGranted = false
			//logger.Printf("voterequest:id:%s from:%v, to: %v, result: %v\n", args.RequestId, args.CandidateId, rf.me, false)
		}
		rf.persist()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//logger.Printf("voterequest:id:%s from:%v, to: %v, result: %v\n", args.RequestId, args.CandidateId, rf.me, false)
	}
	rf.mu.Unlock()
	// Your code here (2A, 2B).
}
func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if arg.Term < rf.currentTerm { //stale
		reply.Term = rf.currentTerm
		reply.Success = false
		//logger.Printf("Stale Leader:id:%s term:%v, from:%v, to:%v, log:%v, lastLogIndex:%v", arg.RequestId, arg.Term, arg.LeaderId, rf.me, len(rf.log), arg.PrevLogIndex)
	} else {
		changed := false
		if rf.currentTerm < arg.Term {
			rf.currentTerm = arg.Term
			rf.matchIndex = nil
			rf.nextIndex = nil
			rf.state = FOLLOWER
			changed = true
		}
		rf.lastHeartBeatTime = time.Now()
		lastLogIndex := 0
		if len(rf.log) > 0 {
			lastLogIndex = len(rf.log) + rf.lastIncludedIndex
		} else {
			lastLogIndex = rf.lastIncludedIndex
		}
		//assumption: arg.PrevLogIndex is assumed to greater than lastLogIndex
		if (rf.lastIncludedIndex == arg.PrevLogIndex && rf.lastIncludedTerm == arg.PrevLogTerm) || (lastLogIndex >= arg.PrevLogIndex && rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term == arg.PrevLogTerm) {
			rf.log = rf.log[0 : arg.PrevLogIndex-rf.lastIncludedIndex]
			rf.log = append(rf.log, arg.Log...)
			if len(arg.Log) > 0 {
				changed = true
			}
			if arg.LeaderCommit > rf.commitIndex {
				rf.commitIndex = int(math.Min(float64(arg.LeaderCommit), float64(rf.lastIncludedIndex+len(rf.log))))
			}
			reply.Success = true
		} else {
			reply.Success = false
			var firstIndexLastLogTerm int
			var lastLogTerm int
			if lastLogIndex < arg.PrevLogIndex { //not that much record
				firstIndexLastLogTerm = lastLogIndex + 1
				//lastLogTerm = rf.log[len(rf.log)-1].Term

			} else { //conflict
				firstIndexLastLogTerm = arg.PrevLogIndex - rf.lastIncludedIndex - 1
				lastLogTerm = rf.log[firstIndexLastLogTerm].Term
				for firstIndexLastLogTerm > 0 {
					if rf.log[firstIndexLastLogTerm-1].Term == lastLogTerm {
						firstIndexLastLogTerm--
					} else {
						break
					}
				}
				firstIndexLastLogTerm += rf.lastIncludedIndex + 1
			}
			reply.FirstLogIndex = firstIndexLastLogTerm
			reply.LastLogTerm = lastLogTerm
			//logger.Printf("Not Match:id:%s term:%v, from:%v, to:%v, log:%v, lastLogIndex:%v,resultIndex:%v,resultTerm:%v", arg.RequestId, arg.Term, arg.LeaderId, rf.me, len(rf.log), arg.PrevLogIndex, firstIndexLastLogTerm, lastLogTerm)
		}
		if changed {
			rf.persist()
		}
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
}

// func (rf *Raft) commitMsg(from int, to int) {
// 	for i := from - rf.lastIncludedIndex - 1; i <= to-rf.lastIncludedIndex-1; i++ {
// 		msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i + rf.lastIncludedIndex + 1}
// 		rf.applyChan <- msg
// 		//logger.Println(fmt.Sprintf("server: %v commit log index :%v", rf.me, i+rf.lastIncludedIndex+1))
// 	}
// }

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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", &args, reply)
	return ok
}
func (rf *Raft) getVotes() {
	args := RequestVoteArgs{}
	args.CandidateId = rf.me
	if len(rf.log) == 0 {
		args.LastLogTerm = rf.lastIncludedTerm
	} else {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	args.LastLogIndex = rf.lastIncludedIndex + len(rf.log)
	args.Term = rf.currentTerm + 1
	args.RequestId = uuid.NewString()
	rf.votedFor = rf.me
	rf.persist()
	voteChanel := make(chan *RequestVoteReply)
	for i := 0; i < len(rf.peers); i++ {
		dest := i
		if dest != rf.me {
			go func() {
				//logger.Println(fmt.Sprintf("GetVoteRquest: from: %v, to:%v, term:%v", rf.me, dest, args.Term))
				reply := &RequestVoteReply{}
				if ok := rf.sendRequestVote(dest, args, reply); ok {
					voteChanel <- reply
				} else {
					voteChanel <- nil
				}

			}()
		}
	}
	go func() {
		receiveVoteCount := 1 //self
		responseCount := 0
		for rf.killed() == false {
			reply := <-voteChanel
			responseCount++
			if reply != nil {
				rf.mu.Lock()
				if rf.currentTerm < args.Term {
					rf.currentTerm = args.Term
					rf.persist()
				}
				rf.mu.Unlock()
				if reply.VoteGranted {
					receiveVoteCount++
					if receiveVoteCount > len(rf.peers)/2 {
						rf.mu.Lock()
						if rf.state == CANDIDATE {
							rf.mu.Unlock()
							rf.initializeLeaderState()
						} else {
							rf.mu.Unlock()
						}
						break
					}
				}
			}
			if responseCount == len(rf.peers)-1 {
				break
			}
		}
	}()
}
func (rf *Raft) initializeLeaderState() {
	rf.mu.Lock()
	//fmt.Println(rf.me)
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = len(rf.log) + rf.lastIncludedIndex
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex + 1
	}
	rf.startHeartBeat()
	go rf.updateCommitted(rf.nextIndex[rf.me])
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitted(commitableIndex int) {
	for {
		rf.mu.Lock()
		if rf.state == LEADER {
			goal := 0
			if commitableIndex > rf.commitIndex {
				goal = commitableIndex
			} else {
				goal = rf.commitIndex + 1
			}
			count := 0
			for i := 0; i < len(rf.matchIndex); i++ {
				if rf.matchIndex[i] >= goal {
					count++
				}
				if count > len(rf.peers)/2 {
					rf.commitIndex = goal
					break
				}
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		} else {
			rf.mu.Unlock()
			break
		}
	}

}
func (rf *Raft) startHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		dest := i
		if dest != rf.me {
			go func() {
				for rf.killed() == false {
					rf.mu.Lock()
					if rf.state != LEADER {
						rf.mu.Unlock()
						break
					} else if rf.matchIndex[dest] >= rf.lastIncludedIndex {
						args := rf.BuildAppendEntriesArgs(dest)
						rf.mu.Unlock()
						go rf.heartBeat(dest, args)
					} else {
						args := &InstallSnapshotArgs{
							LeaderId:          rf.me,
							LastIncludedIndex: rf.lastIncludedIndex,
							LastIncludedTerm:  rf.lastIncludedTerm,
							Term:              rf.currentTerm,
							Data:              rf.persister.snapshot,
						}
						rf.mu.Unlock()
						go rf.sendInstallSnapshot(args, dest)
					}
					time.Sleep(time.Millisecond * 100)
				}
			}()
		}
	}
}

func (rf *Raft) heartBeat(dest int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if len(args.Log) == 0 {
		//logger.Println(fmt.Sprintf("HeartBeat:id：%s from: %v, to:%v term:%v commit:%v", args.RequestId, rf.me, dest, args.Term, args.LeaderCommit))
	} else {
		//logger.Println(fmt.Sprintf("AppendEntries:id:%s from: %v, to:%v term:%v args:%v", args.RequestId, rf.me, dest, args.Term, args))
	}

	success := rf.peers[dest].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	if success && rf.state == LEADER {
		if reply.Success && args.Term == rf.currentTerm {
			rf.nextIndex[dest] = args.PrevLogIndex + len(args.Log) + 1
			rf.matchIndex[dest] = args.PrevLogIndex + len(args.Log)
		} else if args.Term == rf.currentTerm {
			if reply.Term > rf.currentTerm {
				//logger.Println(fmt.Sprintf("Leader: id:%s AppendEntries: from: %v, to:%v  result: stale leader", args.RequestId, rf.me, dest))
				rf.nextIndex = nil
				rf.matchIndex = nil
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.persist()
			} else {
				//logger.Println(fmt.Sprintf("Leader: id:%s AppendEntries: curTerm:%v, argterm:%v, from: %v, to:%v， result: not match, first index:%v, last term:%v", args.RequestId, rf.currentTerm, args.Term, rf.me, dest, reply.FirstLogIndex, reply.LastLogTerm))
				rf.nextIndex[dest] = reply.FirstLogIndex
			}
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(args *InstallSnapshotArgs, dest int) {
	reply := &InstallSnapshotReply{}
	//logger.Printf("Leader:InstallSnapshot from:%v, to %v, lastIndex:%v, lastTerm:%v", args.LeaderId, dest, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[dest].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	if ok {
		if rf.state == LEADER {
			if reply.Term > rf.currentTerm {
				rf.state = FOLLOWER
				rf.nextIndex = nil
				rf.matchIndex = nil
			} else {
				if rf.nextIndex[dest] < args.LastIncludedIndex+1 {
					rf.nextIndex[dest] = args.LastIncludedIndex + 1
				}
				if rf.matchIndex[dest] < args.LastIncludedIndex {
					rf.matchIndex[dest] = args.LastIncludedIndex
				}
			}
		}
	}
	rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		//logger.Println(fmt.Sprintf("Start: at:%v, command:%v", rf.me, command))

		logEntry := LogEntry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		index := rf.lastIncludedIndex + len(rf.log)
		rf.matchIndex[rf.me] = rf.lastIncludedIndex + len(rf.log)
		return index, rf.currentTerm, true
	} else {
		return -1, rf.currentTerm, false
	}
}

func (rf *Raft) BuildAppendEntriesArgs(dest int) *AppendEntriesArgs {
	args := &AppendEntriesArgs{}
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	args.Log = rf.log[rf.nextIndex[dest]-rf.lastIncludedIndex-1:]
	args.PrevLogIndex = rf.nextIndex[dest] - 1
	if rf.nextIndex[dest]-rf.lastIncludedIndex <= 1 {
		args.PrevLogTerm = rf.lastIncludedTerm
	} else {
		args.PrevLogTerm = rf.log[rf.nextIndex[dest]-rf.lastIncludedIndex-2].Term
	}
	args.Term = rf.currentTerm
	args.RequestId = uuid.NewString()
	return args
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		sleepTime := rand.Int31n(150)
		time.Sleep(time.Millisecond * time.Duration(sleepTime+350))
		rf.mu.Lock()
		if (time.Now().UnixNano()-rf.lastHeartBeatTime.UnixNano())/1000000 > int64(sleepTime+350) && rf.state != LEADER {
			rf.state = CANDIDATE
			rf.getVotes()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) applyCommit() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command, CommandIndex: rf.lastApplied}
			rf.applyChan <- msg
			//logger.Println(fmt.Sprintf("server: %v commit log index :%v val:%v", rf.me, rf.lastApplied, rf.log[rf.lastApplied-rf.lastIncludedIndex-1]))
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
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
	rf.applyChan = applyCh
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.state = FOLLOWER
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: "empty log"})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommit()
	return rf
}
