package kvraft

const (
	OK               = "OK"
	ErrNotRunning    = "ErrNotRunning"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrCommitFail    = "ErrCommitFail"
	ErrCommitTimeout = "ErrCommitTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// helper
func Assert(f bool, msg string) {
	if !f {
		panic(msg)
	}
}
