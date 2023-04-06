package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		kvlog.Printf("\033[1;44m[KVServer%d]\033[0m: %s", kv.me, fmt.Sprintf(format, a...))
	}
	return
}

type Op struct {
	Name  string
	Key   string
	Value string

	ClientId int64
	Seq      int64
}

// key-value state machine
type KVSM interface {
	Get(key string) (string, Err)
	Put(key string, value string) Err
	Append(key string, value string) Err
}

type PendingOp struct {
	op      Op
	replyCh chan PendingReply
}

type PendingReply struct {
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm             KVSM
	lastSeq        map[int64]int64
	pendingReplies map[int]PendingOp
	lastApplied    int // for debug
}

const applyTimeout = 150 * time.Millisecond

func (kv *KVServer) Do(args *DoArgs, reply *DoReply) {
	op := Op{
		Name:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		Seq:      args.Seq,
	}

	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	pendingOp := PendingOp{
		op:      op,
		replyCh: make(chan PendingReply),
	}
	kv.pendingReplies[index] = pendingOp
	kv.mu.Unlock()

	select {
	case <-time.After(applyTimeout):
		reply.Err = ErrTimeout
	case r := <-pendingOp.replyCh:
		reply.Err = r.err
		reply.Value = r.value
	}

	kv.DPrintf("got reply: %+v", reply)

	kv.mu.Lock()
	delete(kv.pendingReplies, index)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		// early catch of error in raft, this error may not appear if raft work correctly
		if kv.lastApplied >= m.CommandIndex {
			log.Fatalf("lastApplied=%d > nextIndex=%d", kv.lastApplied, m.CommandIndex)
		} else {
			kv.lastApplied = m.CommandIndex
		}

		op := m.Command.(Op)
		kv.mu.Lock()
		pendingOp, pending := kv.pendingReplies[m.CommandIndex]
		kv.mu.Unlock()

		dup := false
		if op.Name != "Get" { // remove update duplicate
			if kv.lastSeq[op.ClientId] >= op.Seq {
				dup = true
			} else {
				kv.lastSeq[op.ClientId] = op.Seq
			}
		}

		if dup { // duplicate update
			if pending {
				pendingOp.replyCh <- PendingReply{err: OK}
			}
			continue
		}
		reply := PendingReply{}
		if !dup {
			switch op.Name {
			case "Get":
				reply.value, reply.err = kv.sm.Get(op.Key)
				kv.DPrintf("Get(%s)=%s", op.Key, reply.value)
			case "Put":
				reply.err = kv.sm.Put(op.Key, op.Value)
				kv.DPrintf("Put(%s, %s)", op.Key, op.Value)
			case "Append":
				reply.err = kv.sm.Append(op.Key, op.Value)
				kv.DPrintf("Append(%s, %s)", op.Key, op.Value)
			default:
				reply.err = ErrInvalidOp
				kv.DPrintf("Invalid Op: %s", op.Name)
			}
		}
		// original command is replaced by new leader's new command
		// so apply the new command (has apply above) and report error
		// to RPC to retry the original command
		if pendingOp.op != op {
			reply.err = ErrWrongLeader
		}
		if pending {
			pendingOp.replyCh <- reply
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.sm = newMemKV()
	kv.pendingReplies = make(map[int]PendingOp)
	kv.lastSeq = make(map[int64]int64)
	go kv.applier()

	return kv
}

// in-mem key-value state machine
type memKV struct {
	data map[string]string
}

func newMemKV() *memKV {
	return &memKV{
		data: make(map[string]string),
	}
}

func (sm *memKV) Get(key string) (string, Err) {
	if value, ok := sm.data[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (sm *memKV) Put(key string, value string) Err {
	sm.data[key] = value
	return OK
}

func (sm *memKV) Append(key string, value string) Err {
	sm.data[key] += value
	return OK
}
