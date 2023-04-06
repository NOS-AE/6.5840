package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int64
	seq      int64

	leader int // small optimization for stable network in debug, let clerk start polling from the last leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

const doTimeout = 150 * time.Millisecond

func (ck *Clerk) Get(key string) string {
	// kvlog.Println("Get(" + key + ")")
	ck.seq++
	for ; true; ck.leader = (ck.leader + 1) % len(ck.servers) {
		args := DoArgs{
			Op:       "Get",
			Key:      key,
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
		reply := DoReply{}
		ok := ck.DoWithTimeout(ck.leader, &args, &reply, doTimeout)
		if !ok {
			// kvlog.Printf("not ok at [%d]", i)
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err != OK {
			continue
		}
		return reply.Value
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// kvlog.Println(op + "(" + key + "," + value + ")")
	ck.seq++
	for ; true; ck.leader = (ck.leader + 1) % len(ck.servers) {
		args := DoArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
			Key:      key,
			Value:    value,
			Op:       op,
		}
		reply := DoReply{}
		ok := ck.DoWithTimeout(ck.leader, &args, &reply, doTimeout)
		if !ok {
			// kvlog.Printf("not ok at [%d]", i)
			continue
		}
		if reply.Err != OK {
			continue
		}
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) DoWithTimeout(server int, args *DoArgs, reply *DoReply, timeout time.Duration) (ret bool) {
	ch := make(chan bool, 1)
	go func() {
		ch <- ck.servers[server].Call("KVServer.Do", args, reply)
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
