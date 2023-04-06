package kvraft

import (
	"log"
	"os"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrInvalidOp   = "ErrInvalidOp"
	ErrStaleOp     = "ErrStaleOp"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type DoArgs struct {
	Op    string
	Key   string
	Value string

	ClientId int64
	Seq      int64
}

type DoReply struct {
	Err   Err
	Value string
}

var kvlog *log.Logger

func init() {
	file, err := os.OpenFile("kvraft.log.ans", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0777)
	if err != nil {
		log.Fatal(err)
	}
	kvlog = log.New(file, "", log.Ltime|log.Lmicroseconds|log.Lshortfile)
}
