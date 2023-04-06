package raft

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	Follower = iota
	Candidate
	Leader
)

type Entry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state       int
	heartbeatCh chan struct{}
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond
	isPeerResps []bool

	currentTerm int
	votedFor    int
	log         []Entry // index starts from 1

	commitIndex int
	lastApplied int

	nextIndex []int
	// matchIndex []int

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

const logEnable = true
const logToFile = true

func init() {
	if logToFile {
		file, err := os.OpenFile("raft.log.ans", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
		if err != nil {
			log.Fatal(err)
		}
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
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
	if state == Follower && rf.state == Follower {
		return
	}
	if rf.state == Follower && state == Leader {
		log.Fatal("follower->leader")
	}
	if rf.state == Leader && state == Candidate {
		log.Fatal("leader->candidate")
	}
	if logEnable {
		var from string
		var to string
		if rf.state == Follower {
			from = "follower"
		} else if rf.state == Candidate {
			from = "candidate"
		} else {
			from = "leader"
		}
		if state == Follower {
			to = "follower"
		} else if state == Candidate {
			to = "candidate"
		} else {
			to = "leader"
		}
		rf.debug(from + "->" + to)
	}

	rf.state = state
}

func (rf *Raft) setStale(term int, heartbeat bool) {
	rf.setState(Follower)
	if rf.currentTerm != term {
		rf.currentTerm = term
		rf.persist()
	}
	if heartbeat {
		rf.heartbeatCh <- struct{}{}
	}
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

func (rf *Raft) at(index int) Entry {
	return rf.log[rf.i2p(index)]
}

func (rf *Raft) i2p(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

func (rf *Raft) p2i(pos int) (index int) {
	return pos + rf.lastIncludedIndex + 1
}

func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	enc.Encode(rf.lastIncludedIndex)
	enc.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)
	dec.Decode(&rf.log)
	dec.Decode(&rf.lastIncludedIndex)
	dec.Decode(&rf.lastIncludedTerm)
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	p := rf.i2p(index + 1)
	rf.lastIncludedTerm = rf.at(index).Term
	rf.lastIncludedIndex = index
	rf.log = rf.log[p:]
	rf.snapshot = snapshot
	rf.debug("snapshot at %d", index)
	rf.persist()
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.setStale(args.Term, true)
	p := rf.i2p(args.LastIncludedIndex)
	if len(rf.log)-1 <= p {
		rf.log = nil
	} else {
		rf.log = rf.log[p:]
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.persist()
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.debug("install snapshot: index=%d, term=%d", applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
	rf.applyCh <- applyMsg
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
		lastLogTerm := rf.getLastLogTerm()
		if args.LastLogTerm > lastLogTerm {
			return true
		} else if args.LastLogTerm < lastLogTerm {
			return false
		} else {
			return args.LastLogIndex >= rf.p2i(len(rf.log)-1)
		}
	}
	grant := func() {
		reply.VoteGranted = true
		if rf.votedFor != args.CandidateId {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
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
	isHeartbeat := false
	if args.Term > rf.currentTerm || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if checkLogUptodate() {
			isHeartbeat = true
			grant()
		} else {
			notGrant()
		}
	} else {
		notGrant()
	}
	rf.setStale(args.Term, isHeartbeat)
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

	XTerm  int // term in the conflicting entry
	XIndex int // index of first entry with that term
	XLen   int // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.setStale(args.Term, true)
	if len(args.Entries) > 0 {
		rf.debug("rcv appendEntries %+v", *args)
	} else {
		// rf.debug("rcv heartbeat %v", args)
	}

	nextIndex := rf.p2i(len(rf.log))
	if nextIndex <= args.PrevLogIndex { // entry not exist
		rf.debug("not exist, length=%d, require=%d", nextIndex, args.PrevLogIndex)
		reply.Success = false
		reply.XLen = nextIndex
	} else { // entry conflict with leader
		prevLogPos := rf.i2p(args.PrevLogIndex)
		var prevLogTerm int
		if prevLogPos >= 0 {
			prevLogTerm = rf.log[prevLogPos].Term
		} else if prevLogPos < -1 {
			log.Fatal("prevLogPos < -1")
		} else {
			prevLogTerm = rf.lastIncludedTerm
		}
		if prevLogTerm != args.PrevLogTerm {
			rf.debug("conflict at %d, term=%v, required=%+v, log=%+v", args.PrevLogIndex, rf.at(args.PrevLogIndex), args.PrevLogTerm, rf.log)
			reply.Success = false
			reply.XIndex = args.PrevLogIndex
			reply.XTerm = rf.at(reply.XIndex).Term
			for reply.XIndex > 0 && rf.at(reply.XIndex-1).Term == reply.XTerm {
				reply.XIndex--
			}
			reply.XLen = nextIndex
			rf.log = rf.log[:rf.i2p(args.PrevLogIndex)]
			rf.persist()
		} else { // append entry
			rf.debug("append entry %+v", args)
			diff := nextIndex > args.PrevLogIndex+1 || len(args.Entries) > 0
			rf.log = rf.log[:rf.i2p(args.PrevLogIndex+1)]
			rf.log = append(rf.log, args.Entries...)
			if diff {
				rf.persist()
			}
			reply.Success = true
		}
	}
	reply.Term = args.Term
	if reply.Success {
		if args.LeaderCommit < rf.p2i(len(rf.log)-1) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.p2i(len(rf.log) - 1)
		}
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func sendWithTimeout(f func() bool, timeout time.Duration) (ret bool) {
	ch := make(chan bool, 1)
	go func() {
		ch <- f()
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case ret = <-ch:
	case <-timer.C:
		ret = false
	}
	return
}

// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.p2i(len(rf.log))
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (2B).
	if isLeader {
		rf.debug("start %+v", command)
		rf.log = append(rf.log, Entry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.persist()
		// rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const heartbeatInterval = 150 * time.Millisecond
const checkReplicateInterval = 10 * time.Millisecond

// get prevLogTerm or install snapshot
// locked state is held entering and returning this function
func (rf *Raft) getPrevLogTerm(server int, term int) (prevLogTerm int, isStale bool) {
	index := rf.nextIndex[server]
	prevLogPos := rf.i2p(index - 1)
	if prevLogPos >= 0 {
		return rf.log[prevLogPos].Term, false
	} else if prevLogPos == -1 {
		return rf.lastIncludedTerm, false
	}
	snapshot := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()
	ok := sendWithTimeout(func() bool {
		return rf.sendInstallSnapshot(server, &args, &reply)
	}, heartbeatInterval/2)
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term || rf.killed() || index != rf.nextIndex[server] {
		return -1, true
	}
	if ok && reply.Term > term {
		rf.setStale(reply.Term, true)
		return -1, true
	} else {
		rf.isPeerResps[server] = true
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
		return -1, false
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		if rf.lastIncludedTerm < 0 {
			log.Fatal("lastIncludedTerm < 0")
		}
		return rf.lastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) handleReplicate(server int, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextTick := time.Now()
	for !rf.killed() && rf.state == Leader && rf.currentTerm == term {
		if rf.nextIndex[server] == rf.nextIndex[rf.me] && nextTick.After(time.Now()) {
			rf.mu.Unlock()
			time.Sleep(checkReplicateInterval)
			rf.mu.Lock()
			continue
		}
		if rf.nextIndex[server] > rf.nextIndex[rf.me] {
			log.Fatalf("rf.nextIndex[server] > len(rf.log): %+v", rf.nextIndex)
		}
		rf.debug("%+v, %d, %d", rf.nextIndex, rf.lastIncludedIndex, len(rf.log))
		prevLogTerm, isStale := rf.getPrevLogTerm(server, term)
		if isStale {
			return
		}
		if prevLogTerm == -1 {
			continue // installation
		}
		index := rf.nextIndex[server]
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: index - 1,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		if index < rf.nextIndex[rf.me] {
			args.Entries = make([]Entry, rf.nextIndex[rf.me]-index)
			copy(args.Entries, rf.log[rf.i2p(index):rf.i2p(rf.nextIndex[rf.me])])
		}
		size := len(args.Entries)
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		nextTick = time.Now().Add(heartbeatInterval)
		ok := sendWithTimeout(func() bool {
			return rf.sendAppendEntries(server, &args, &reply)
		}, heartbeatInterval/2)
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term || rf.killed() || index != rf.nextIndex[server] {
			return
		}
		if !ok {
			rf.mu.Unlock()
			time.Sleep(heartbeatInterval / 2) // sleep a while before retry
			rf.mu.Lock()
			continue
		}
		rf.isPeerResps[server] = true
		if !reply.Success {
			rf.setStale(reply.Term, true)
			return
		}
		if size > 0 {
			// rf.debug("replicate success: %+v", rf.nextIndex)
			index += size
			rf.nextIndex[server] = index
			index--
			if rf.commitIndex >= index {
				continue
			}
			if rf.at(index).Term == term { // 5.4.2
				replicaAtIndex := 0
				for _, i := range rf.nextIndex {
					if i > index {
						replicaAtIndex++
					}
				}
				if replicaAtIndex >= rf.majority() {
					rf.commitIndex = index
					rf.applyCond.Signal()
				}
			}
		}
	}
}

// return true if stale
func (rf *Raft) consistencyCheck(server int, term int) (isStale bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() && rf.state == Leader && rf.currentTerm == term {
		// rf.debug("decrement[%d], %+v\n", server, rf.nextIndex)
		prevLogTerm, isStale := rf.getPrevLogTerm(server, term)
		if isStale {
			return true
		}
		if prevLogTerm == -1 {
			continue // installation
		}
		index := rf.nextIndex[server]
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: index - 1,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		ok := sendWithTimeout(func() bool {
			return rf.sendAppendEntries(server, &args, &reply)
		}, heartbeatInterval/2)
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != Leader || rf.killed() {
			return true
		}
		if !ok {
			rf.debug("timeout serving[%d]", server)
			continue
		}
		// got reply
		rf.debug("got reply[%d]: %+v", server, reply)
		rf.isPeerResps[server] = true
		if !reply.Success && reply.Term > term {
			rf.setStale(reply.Term, true)
			return true
		}
		if reply.Success {
			break
		}
		if reply.XLen < index { // case3
			rf.nextIndex[server] = reply.XLen
		} else {
			index--
			for rf.i2p(index) >= 0 && rf.at(index).Term != reply.XTerm {
				index--
			}
			if rf.i2p(index) >= 0 || rf.i2p(index) == -1 && rf.lastIncludedTerm == reply.XTerm { // case 2
				rf.nextIndex[server] = index + 1
			} else { // case 1
				rf.debug("no term %d", reply.XTerm)
				rf.nextIndex[server] = reply.XIndex
			}
		}
		// rf.nextIndex[server]--
	}
	// rf.matchIndex[server] = rf.nextIndex[server] - 1
	rf.debug("consistency at server[%d] reach: %+v", server, rf.nextIndex)

	return false
}

func (rf *Raft) servePeer(server int, term int) {
	if isStale := rf.consistencyCheck(server, term); isStale {
		return
	}
	go rf.handleReplicate(server, term)
}

func (rf *Raft) startLeader(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate || rf.currentTerm != term {
		return
	}
	rf.setState(Leader)
	for server := range rf.peers {
		rf.nextIndex[server] = rf.p2i(len(rf.log))
		if server != rf.me {
			go rf.servePeer(server, rf.currentTerm)
		}
	}

	// leader step down if an election timeout elapses
	// without a successful round of heartbeats to a majorty of it's cluster
	ticker := time.NewTicker(genElectionTimeout())
	defer ticker.Stop()
	rf.mu.Unlock()
	for range ticker.C {
		rf.mu.Lock()
		if rf.currentTerm != term || rf.state != Leader {
			return
		}
		// respCount := 1
		// for i, r := range rf.isPeerResps {
		// 	if r {
		// 		respCount++
		// 	}
		// 	rf.isPeerResps[i] = false
		// }
		// if respCount < rf.majority() {
		// 	rf.staleCh <- staleMsg{rf.currentTerm, true}
		// 	rf.mu.Unlock()
		// 	return
		// }
		rf.mu.Unlock()
		ticker.Reset(genElectionTimeout())
	}
}

func (rf *Raft) startElection(term int) {
	rf.mu.Lock()
	rf.setState(Candidate)
	rf.votedFor = rf.me
	rf.persist()
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
				go rf.startLeader(term)
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
		lastLogIndex := rf.p2i(len(rf.log) - 1)
		lastLogTerm := rf.getLastLogTerm()
		if lastLogIndex < -1 {
			log.Fatal("lastLogIndex < -1")
		}
		args := RequestVoteArgs{
			Term:         term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		ok := sendWithTimeout(func() bool {
			return rf.sendRequestVote(server, &args, &reply)
		}, heartbeatInterval)
		rf.mu.Lock()
		if term != rf.currentTerm || rf.state != Candidate || rf.killed() {
			return
		}
		if ok {
			if reply.VoteGranted {
				grantCh <- struct{}{}
			} else {
				if term < reply.Term {
					rf.setStale(reply.Term, true)
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
				rf.currentTerm++
				rf.persist()
				go rf.startElection(rf.currentTerm)
			}
			rf.mu.Unlock()
			ticker.Reset(genElectionTimeout())
		case <-rf.heartbeatCh:
			ticker.Reset(genElectionTimeout())
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.at(rf.lastApplied).Command,
		}
		rf.debug("commit %+v", applyMsg)
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
}

// 300~499ms
func genElectionTimeout() time.Duration {
	return time.Duration(rand.Int()%200+300) * time.Millisecond
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.heartbeatCh = make(chan struct{}, len(peers))
	rf.state = Follower
	rf.log = make([]Entry, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.isPeerResps = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.lastIncludedIndex == 0 {
		rf.lastIncludedIndex = -1
		rf.lastIncludedTerm = -1
	} else if rf.lastIncludedIndex != -1 {
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.snapshot = persister.ReadSnapshot()
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotIndex: rf.lastIncludedIndex,
			SnapshotTerm:  rf.lastIncludedTerm,
		}
		rf.debug("install snapshot: index=%d, term=%d", applyMsg.SnapshotIndex, applyMsg.SnapshotTerm)
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
