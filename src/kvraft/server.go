package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	// ms
	waitCommitTimeout = 1000
)

type OpType int

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
}

type Command struct {
	Op        Op
	LaunchId  int
	LaunchSeq uint64
}

type Result struct {
	Key string
	Val string
	Err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataImpl *KVStore
	opChans  map[uint64]chan Result
	cmdSeq   uint64
}

func (kv *KVServer) doOp(op Op) Result {
	var res Result
	switch op.Type {
	case OpTypeGet:
		// Get
		val, ok := kv.dataImpl.Get(op.Key)
		if !ok {
			res = Result{Err: ErrNoKey}
		} else {
			res = Result{Key: op.Key, Val: val, Err: OK}
		}
	case OpTypePut:
		kv.dataImpl.Put(op.Key, op.Value)
		res = Result{Err: OK}
	case OpTypeAppend:
		kv.dataImpl.Append(op.Key, op.Value)
		res = Result{Err: OK}
	default:
		Assert(false, "doOp: unknown op type")
	}

	return res
}

// RSM，读取applyCh，针对执行信息返回
func (kv *KVServer) readApply() {
	for {
		applyMsg := <-kv.applyCh
		_, cmd := applyMsg.CommandIndex, applyMsg.Command.(Command)

		res := kv.doOp(cmd.Op)

		if cmd.LaunchId != kv.me {
			continue
		}

		kv.opChans[cmd.LaunchSeq] <- res
		DPrintf("KVServer %v apply index %v, cmd %+v, res %+v", kv.me, applyMsg.CommandIndex, cmd, res)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrNotRunning
		return
	}

	kv.mu.Lock()

	// 调用start，如果是leader
	cmd := Command{
		Op:        Op{Type: OpTypeGet, Key: args.Key},
		LaunchId:  kv.me,
		LaunchSeq: kv.cmdSeq,
	}
	kv.opChans[cmd.LaunchSeq] = make(chan Result, 1)
	kv.cmdSeq++

	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Unlock()

	timer := time.After(time.Duration(waitCommitTimeout) * time.Millisecond)
	for {
		select {
		case <-timer:
			reply.Err = ErrCommitTimeout
			DPrintf("Get timeout")
			return
		case res := <-kv.opChans[cmd.LaunchSeq]:
			if res.Err == OK {
				close(kv.opChans[cmd.LaunchSeq])
				delete(kv.opChans, cmd.LaunchSeq)
				reply.Err = OK
				reply.Value = res.Val
			} else {
				reply.Err = res.Err
			}
			return
			// 如果已经不是Leader了。返回让client re-send
			// default:
			// 判断当前是否是leader
		}
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrNotRunning
		return
	}

	kv.mu.Lock()

	// 调用start，如果是leader
	var optype OpType
	if args.Op == "Put" {
		optype = OpTypePut
	} else if args.Op == "Append" {
		optype = OpTypeAppend
	}
	cmd := Command{
		Op:        Op{Type: optype, Key: args.Key, Value: args.Value},
		LaunchId:  kv.me,
		LaunchSeq: kv.cmdSeq,
	}
	kv.opChans[cmd.LaunchSeq] = make(chan Result, 1)
	kv.cmdSeq++

	_, _, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		kv.mu.Unlock()
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Unlock()

	timer := time.After(time.Duration(waitCommitTimeout) * time.Millisecond)
	for {
		select {
		case <-timer:
			reply.Err = ErrCommitTimeout
			DPrintf("Get timeout")
			return
		case res := <-kv.opChans[cmd.LaunchSeq]:
			reply.Err = res.Err
			close(kv.opChans[cmd.LaunchSeq])
			delete(kv.opChans, cmd.LaunchSeq)
			return
		}
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
	labgob.Register(Command{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.dataImpl = NewKVStore()
	kv.cmdSeq = 0

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opChans = make(map[uint64]chan Result)

	// You may need initialization code here.
	go kv.readApply()

	return kv
}
