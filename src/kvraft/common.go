package kvraft

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK               = "OK"
	ErrNotRunning    = "ErrNotRunning"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader" // 本身不是leader
	ErrCommitFail    = "ErrCommitFail"  // 由于Leader变更
	ErrCommitTimeout = "ErrCommitTimeout"
	ErrDupRequest    = "ErrDupRequest" // 重复请求
)

type Err string

type ReqIdentity struct {
	UUID string
}

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	ReqId ReqIdentity
}

type PutAppendReply struct {
	Err   Err
	ReqId ReqIdentity

	// temporary for debug
	Key   string
	Val   string
	Index int
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	ReqId ReqIdentity
}

type GetReply struct {
	Err   Err
	Value string

	ReqId ReqIdentity // 用于唯一标识一个请求
	Index int
}

// helper
func Assert(f bool, msg string) {
	if !f {
		panic(msg)
	}
}
