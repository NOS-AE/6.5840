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
	"log"
	"math/rand"
	"os"
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

const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Term    int
	Command interface{}
}

type staleMsg struct {
	term        int
	resetTicker bool
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
	state   int
	staleCh chan staleMsg
	applyCh chan ApplyMsg

	currentTerm int
	votedFor    int
	log         []Entry // index starts from 1

	commitIndex int
	lastApplied int

	nextIndex []int
	// matchIndex []int
}

const logEnable = true

func init() {
	if logEnable {
		file, err := os.OpenFile("raft.log.ans", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(file)
	}
}

func (rf *Raft) debug(format string, v ...interface{}) {
	if logEnable {
		if rf.mu.TryLock() {
			defer rf.mu.Unlock()
		}
		var s string
		if rf.state == Leader {
			s = "\033[1;41m[Server%d]\033[0m"
		} else if rf.state == Candidate {
			s = "\033[1;43m[Server%d]\033[0m"
		} else {
			s = "\033[1;42m[Server%d]\033[0m"
		}
		log.Printf(s+"(%d): %s", rf.me, rf.currentTerm, fmt.Sprintf(format, v...))
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) setState(state int) {
	if rf.state == Follower && state == Candidate {
		rf.debug("follower->candidate")
	} else if rf.state == Candidate {
		if state == Follower {
			rf.debug("candidate->follower")
		} else if state == Candidate {
			rf.debug("candidate->candidate")
		} else {
			rf.debug("candidate->leader")
		}
	} else if rf.state == Leader && state == Follower {
		rf.debug("leader->follower")
	}
	rf.state = state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
	// Your code here (2A).
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	checkLogUptodate := func() bool {
		lastLog := rf.log[len(rf.log)-1]
		if args.LastLogTerm > lastLog.Term {
			return true
		} else if args.LastLogTerm < lastLog.Term {
			return false
		} else {
			return args.LastLogIndex >= len(rf.log)-1
		}
	}
	grant := func() {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
	notGrant := func() {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		notGrant()
		return
	}
	// rf.debug("requestVote: %+v", *args)
	m := staleMsg{args.Term, false}
	if args.Term > rf.currentTerm || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if checkLogUptodate() {
			m.resetTicker = true
			grant()
		} else {
			notGrant()
		}
	} else {
		notGrant()
	}
	rf.staleCh <- m
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []Entry
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.staleCh <- staleMsg{args.Term, true}
	if len(args.Entries) > 0 {
		rf.debug("rcv %+v", *args)
	}

	if len(rf.log) <= args.PrevLogIndex { // entry not exist
		rf.debug("not exist")
		reply.Success = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // entry conflict with leader
		rf.debug("conflict")
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Success = false
	} else { // append entry
		rf.log = rf.log[:args.PrevLogIndex+1]
		reply.Success = true
		rf.log = append(rf.log, args.Entries...)
	}
	rf.currentTerm = args.Term
	reply.Term = args.Term

	if reply.Success {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

const sendTimeout = heartbeatInterval

func sendWithTimeout(f func() bool) (ret bool) {
	ch := make(chan bool, 1)
	go func() {
		ch <- f()
	}()
	timer := time.NewTimer(sendTimeout)
	defer timer.Stop()
	select {
	case ret = <-ch:
	case <-timer.C:
		ret = false
	}
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (2B).
	if isLeader {
		rf.debug("start %v", command)
		rf.log = append(rf.log, Entry{
			Term:    rf.currentTerm,
			Command: command,
		})
		// rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
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

const heartbeatInterval = 150 * time.Millisecond

func (rf *Raft) heartbeat(server int, term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for ; !rf.killed(); <-ticker.C {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := sendWithTimeout(func() bool {
			return rf.sendAppendEntries(server, &args, &reply)
		})
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if ok {
			if !reply.Success {
				rf.staleCh <- staleMsg{reply.Term, true}
				return
			}
		}
	}
}

const checkReplicateInterval = 20 * time.Millisecond

func (rf *Raft) handleReplicate(server int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() && rf.state == Leader && rf.currentTerm == term {
		if rf.nextIndex[server] == len(rf.log) {
			rf.mu.Unlock()
			time.Sleep(checkReplicateInterval)
			rf.mu.Lock()
			continue
		}
		if rf.nextIndex[server] > len(rf.log) {
			log.Fatalf("rf.nextIndex[server] > len(rf.log): %+v", rf.nextIndex)
		}
		index := rf.nextIndex[server]
		entry := rf.log[index]
		args := AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			Entries: []Entry{
				entry,
			},
			PrevLogIndex: index - 1,
			PrevLogTerm:  rf.log[index-1].Term,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		hasSent := sendWithTimeout(func() bool {
			return rf.sendAppendEntries(server, &args, &reply)
		})
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			return
		}
		if !hasSent {
			rf.mu.Unlock()
			time.Sleep(sendTimeout) // sleep a while before retry
			rf.mu.Lock()
			continue
		}
		if !reply.Success {
			rf.staleCh <- staleMsg{reply.Term, true}
			return
		}
		rf.nextIndex[server]++
		rf.debug("replicate success: %+v", rf.nextIndex)
		if rf.commitIndex >= index {
			continue
		}
		if rf.log[index].Term == rf.currentTerm { // 5.4.2
			replicaAtIndex := 0
			for _, i := range rf.nextIndex {
				if i > index {
					replicaAtIndex++
				}
			}
			if replicaAtIndex == rf.majority() {
				rf.commitIndex = index
			}
		}
	}
}

// return true if stale
func (rf *Raft) consistencyCheck(server int, term int) (isStale bool, isOutdatedTerm bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		rf.debug("decrement[%d], %+v\n", server, rf.nextIndex)
		nextIndex := rf.nextIndex[server]
		prevEntry := rf.log[nextIndex-1]
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  prevEntry.Term,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		ok := sendWithTimeout(func() bool {
			return rf.sendAppendEntries(server, &args, &reply)
		})
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != Leader {
			return false, true
		}
		if !ok {
			continue
		}
		// got reply
		if !reply.Success && reply.Term > term {
			rf.staleCh <- staleMsg{reply.Term, true}
			return true, false
		}
		if reply.Success {
			break
		}
		rf.nextIndex[server]--
	}
	// rf.matchIndex[server] = rf.nextIndex[server] - 1
	rf.debug("consistency at server[%d] reach: %+v", server, rf.nextIndex)

	return false, false
}

func (rf *Raft) servePeer(server int, term int) {
	if isStale, isOutdatedTerm := rf.consistencyCheck(server, term); isStale || isOutdatedTerm {
		return
	}
	go rf.handleReplicate(server, term)
	go rf.heartbeat(server, term)
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setState(Leader)
	for server := range rf.peers {
		rf.nextIndex[server] = len(rf.log)
		if server != rf.me {
			go rf.servePeer(server, rf.currentTerm)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setState(Candidate)
	rf.votedFor = rf.me
	rf.currentTerm++
	term := rf.currentTerm
	rf.mu.Unlock()

	grantCh := make(chan struct{}, len(rf.peers))
	for index := range rf.peers {
		if index != rf.me {
			go rf.requestVote(index, grantCh, term)
		}
	}
	voteNum := 1
	for !rf.killed() {
		select {
		case <-grantCh:
			voteNum++
			if voteNum == rf.majority() {
				go rf.startLeader()
				// return
			}
		default:
			rf.mu.Lock()
			if term != rf.currentTerm || rf.state != Candidate {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}
}

func (rf *Raft) requestVote(server int, grantCh chan<- struct{}, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		ok := sendWithTimeout(func() bool {
			return rf.sendRequestVote(server, &args, &reply)
		})
		rf.mu.Lock()
		if term != rf.currentTerm || rf.state != Candidate {
			return
		}
		if ok {
			if reply.VoteGranted {
				grantCh <- struct{}{}
			} else {
				if term < reply.Term {
					rf.staleCh <- staleMsg{reply.Term, true}
				}
			}
			return
		}
	}
}

func (rf *Raft) ticker() {
	ticker := time.NewTicker(genElectionTimeout())
	defer ticker.Stop()
	for !rf.killed() {
		select {
		case <-ticker.C:
			rf.mu.Lock()
			if rf.state != Leader {
				go rf.startElection()
			}
			rf.mu.Unlock()
		case m := <-rf.staleCh:
			rf.mu.Lock()
			rf.currentTerm = m.term
			rf.setState(Follower)
			rf.votedFor = -1
			rf.mu.Unlock()
			if m.resetTicker {
				ticker.Reset(genElectionTimeout())
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].Command,
			}
			rf.mu.Unlock()
			rf.debug("commit %+v", applyMsg)
			rf.applyCh <- applyMsg
		} else {
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// 300~499ms
func genElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%200+300) * time.Millisecond
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.staleCh = make(chan staleMsg, len(peers))
	rf.votedFor = -1
	rf.state = Follower
	rf.log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	// rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
