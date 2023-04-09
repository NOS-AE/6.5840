package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		kvlog.Printf("\033[1;44m[KVServer%d]\033[0m: %s", kv.me, fmt.Sprintf(format, a...))
	}
	return
}

func (kv *KVServer) DPrintf2(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf("\033[1;44m[KVServer%d]\033[0m: %s", kv.me, fmt.Sprintf(format, a...))
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

	// if value is empty, convert to delete.
	// WARNING: the side effect of ErrNoKey
	// bring to the Get on a previously existed key.
	Put(key string, value string) Err
	Append(key string, value string) Err

	// get entire underlying data
	GetData() map[string]string

	// replace underlying data with data
	SetData(data map[string]string)
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
	sm           KVSM
	lastSeq      map[int64]int64
	pendingReply map[int]chan PendingReply
	pendingOp    map[int]Op
	persister    *raft.Persister
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
	pendingReply := make(chan PendingReply)
	kv.pendingOp[index] = op
	kv.pendingReply[index] = pendingReply
	kv.mu.Unlock()

	select {
	case <-time.After(applyTimeout):
		reply.Err = ErrTimeout
	case r := <-pendingReply:
		reply.Err = r.err
		reply.Value = r.value
	}

	kv.DPrintf("got reply at %d: %+v, op=%+v", index, reply, op)

	kv.mu.Lock()
	delete(kv.pendingOp, index)
	delete(kv.pendingReply, index)
	kv.mu.Unlock()
	select {
	case <-pendingReply:
	default:
	}
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
		if m.CommandValid {
			kv.DPrintf2("got commit: %+v", m)

			op := m.Command.(Op)
			kv.mu.Lock()
			pendingOp, pending := kv.pendingOp[m.CommandIndex]
			kv.mu.Unlock()

			isConflict := false
			// original command is replaced by new leader's new command
			// so apply the new command and report error
			// to RPC to retry the original command
			if pending {
				kv.DPrintf2("check op equal at %d: %+v, %+v", m.CommandIndex, pendingOp, op)
				_, isLeader := kv.rf.GetState()
				if pendingOp.ClientId != op.ClientId || pendingOp.Seq != op.Seq || !isLeader {
					isConflict = true
				}
			}

			isDup := false
			if op.Name != "Get" { // remove update duplicate
				if kv.lastSeq[op.ClientId] >= op.Seq {
					isDup = true
				} else {
					kv.lastSeq[op.ClientId] = op.Seq
				}
			}
			if isDup {
				kv.DPrintf2("dup: %+v", m)
			}

			reply := PendingReply{err: OK}
			if !isDup {
				switch op.Name {
				case "Get":
					reply.value, reply.err = kv.sm.Get(op.Key)
				case "Put":
					reply.err = kv.sm.Put(op.Key, op.Value)
				case "Append":
					reply.err = kv.sm.Append(op.Key, op.Value)
				default:
					reply.err = ErrInvalidOp
				}
				if op.Name != "Get" {
					kv.DPrintf2("after %+v: %+v", m, kv.sm.GetData())
				}
			}

			if isConflict {
				reply.err = ErrWrongLeader
			}

			// snapshot after apply and reply
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.lastSeq)
				e.Encode(kv.sm.GetData())
				kv.DPrintf2("snapshot at %d", m.CommandIndex)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}

			kv.mu.Lock()
			pendingReply := kv.pendingReply[m.CommandIndex]
			if pendingReply != nil {
				pendingReply <- reply
			}
			kv.mu.Unlock()
		} else if m.SnapshotValid {
			kv.installSnapshot(m.Snapshot)
		} else {
			// ignore other type of message
		}
	}
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	dec := labgob.NewDecoder(r)
	var lastSeq map[int64]int64
	var data map[string]string
	dec.Decode(&lastSeq)
	dec.Decode(&data)
	kv.lastSeq = lastSeq
	kv.sm.SetData(data)
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
	kv.pendingOp = make(map[int]Op)
	kv.pendingReply = make(map[int]chan PendingReply)
	kv.lastSeq = make(map[int64]int64)
	kv.persister = persister
	if kv.maxraftstate > 0 && kv.persister.SnapshotSize() > 0 {
		kv.installSnapshot(persister.ReadSnapshot())
	}
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
	if value == "" {
		delete(sm.data, key)
	} else {
		sm.data[key] = value
	}
	return OK
}

func (sm *memKV) Append(key string, value string) Err {
	sm.data[key] += value
	return OK
}

func (sm *memKV) GetData() map[string]string {
	return clone(sm.data)
}

func (sm *memKV) SetData(data map[string]string) {
	sm.data = clone(data)
}

func clone(data map[string]string) map[string]string {
	ret := make(map[string]string)
	for k, v := range data {
		ret[k] = v
	}
	return ret
}
