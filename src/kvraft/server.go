package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	// ms
	waitCommitTimeout   = 1000
	checkLeaderInterval = 10
	// us
	applyInterval = 100
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
	Op       Op
	LaunchId int
	ReqId    ReqIdentity
}

type Result struct {
	Key string
	Val string
	Err Err

	Index int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dataImpl  *KVStore
	opChans   map[ReqIdentity]chan Result // 客户端等待结果的channel
	opHistory map[ReqIdentity]Result      // 执行历史
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
		oldVal, _ := kv.dataImpl.Get(op.Key)
		kv.dataImpl.Put(op.Key, op.Value)
		res = Result{Err: OK}
		curVal, _ := kv.dataImpl.Get(op.Key)
		DPrintf("RSM: kvserver %v append key %v value %v, old val %v, cur value %v", kv.me, op.Key, op.Value, oldVal, curVal)
		res = Result{Key: op.Key, Val: op.Value, Err: OK}
	case OpTypeAppend:
		oldVal, _ := kv.dataImpl.Get(op.Key)
		kv.dataImpl.Append(op.Key, op.Value)
		curVal, _ := kv.dataImpl.Get(op.Key)
		DPrintf("RSM: kvserver %v append key %v value %v, old val %v, cur value %v", kv.me, op.Key, op.Value, oldVal, curVal)
		// res = Result{Err: OK}
		res = Result{Key: op.Key, Val: curVal, Err: OK}
	default:
		Assert(false, "doOp: unknown op type")
	}

	return res
}

// func checkTimeoutChannel(f func()) bool {
// 	timer := time.After(time.Duration(waitCommitTimeout) * 3 * time.Millisecond)
// 	done := make(chan struct{})
// 	go func(c chan struct{}) {
// 		f()
// 		c <- struct{}{}
// 	}(done)
// 	select {
// 	case <-done:
// 		return true
// 	case <-timer:
// 		return false
// 		// Assert(false, "checkTimeoutChannel: timeout")
// 	}

// }

// RSM，读取applyCh，针对执行信息返回
func (kv *KVServer) readApply() {
	for {
		applyMsg := <-kv.applyCh
		index, cmd := applyMsg.CommandIndex, applyMsg.Command.(Command)

		curReqId := cmd.ReqId
		kv.mu.Lock()
		if res, ok := kv.opHistory[curReqId]; ok {

			if cmd.LaunchId != kv.me {
				kv.mu.Unlock()
				continue
			}

			if resChan, f := kv.opChans[cmd.ReqId]; f {
				res.Err = ErrDupRequest
				resChan <- res
			}

			kv.mu.Unlock()
			continue
		}
		kv.mu.Unlock()

		res := kv.doOp(cmd.Op)
		res.Index = index
		// fmt.Printf("OP: KVServer %v doOp cmd:%+v, index:%v, res:%+v\n", kv.me, cmd, index, res)

		kv.mu.Lock()

		kv.opHistory[curReqId] = res
		// DPrintf("APPLY: KVServer %v add history index %v, reqId:%v, launchSeq:%v, appliedIndex:%v, historyLen:%v", kv.me, applyMsg.CommandIndex, cmd.ReqId, cmd.LaunchSeq, kv.appliedIndex, len(kv.opHistory))

		if cmd.LaunchId == kv.me {
			if resChan, f := kv.opChans[cmd.ReqId]; f {
				resChan <- res
			}
		}
		kv.mu.Unlock()

	}
}

func (kv *KVServer) GetDupRes(req ReqIdentity) (Result, bool) {
	if res, ok := kv.opHistory[req]; ok {
		return res, true
	} else {
		return Result{}, false
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// DPrintf("Server: %v get func: key: %v, reqId:%v", kv.me, args.Key, args.ReqId)
	if kv.killed() {
		reply.Err = ErrNotRunning
		return
	}

	kv.mu.Lock()
	if res, ok := kv.GetDupRes(args.ReqId); ok {
		reply.Err = ErrDupRequest
		reply.Value = res.Val
		kv.mu.Unlock()
		return
	}

	cmd := Command{
		Op:       Op{Type: OpTypeGet, Key: args.Key},
		LaunchId: kv.me,
		ReqId:    args.ReqId,
	}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	resChan := make(chan Result, 1)
	kv.opChans[cmd.ReqId] = resChan
	defer func() {
		kv.mu.Lock()
		close(resChan)
		delete(kv.opChans, cmd.ReqId)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()

	DPrintf("Server: %v get func as leader: key: %v, reqId:%v, index:%v", kv.me, args.Key, args.ReqId, index)
	timer := time.After(time.Duration(waitCommitTimeout) * time.Millisecond)
	for {
		select {
		case <-timer:
			reply.Err = ErrCommitTimeout
			// DPrintf("Server: %v get timeout waiting commit, cmd:%+v", kv.me, cmd)
			return
		case res := <-resChan:
			reply.Err = res.Err
			reply.Index = res.Index
			if res.Err == OK || res.Err == ErrDupRequest {
				reply.Value = res.Val
				// DPrintf("Server: %v get return key: %v, reply: %+v", kv.me, args.Key, reply.Err)
			}
			Assert(res.Err == OK || res.Err == ErrDupRequest || res.Err == ErrNoKey, "Get: res return but not OK or ErrDupRequest or ErrNoKey")
			return
		case <-time.After(time.Duration(checkLeaderInterval) * time.Millisecond):
			curTerm, curIsLeader := kv.rf.GetState()
			if curTerm != term || !curIsLeader {
				reply.Err = ErrCommitFail
				return
			}
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
	if _, ok := kv.GetDupRes(args.ReqId); ok {
		reply.Err = ErrDupRequest
		kv.mu.Unlock()
		return
	}

	var optype OpType
	if args.Op == "Put" {
		optype = OpTypePut
	} else if args.Op == "Append" {
		optype = OpTypeAppend
	}

	cmd := Command{
		Op:       Op{Type: optype, Key: args.Key, Value: args.Value},
		LaunchId: kv.me,
		ReqId:    args.ReqId,
	}

	index, term, isLeader := kv.rf.Start(cmd)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// DPrintf("Server: %v put func as leader: key: %v, reqId:%v, index:%v", kv.me, args.Key, args.ReqId, index)
	resChan := make(chan Result, 1)
	kv.opChans[cmd.ReqId] = resChan
	defer func() {
		kv.mu.Lock()
		close(resChan)
		delete(kv.opChans, cmd.ReqId)
		kv.mu.Unlock()
	}()

	kv.mu.Unlock()

	DPrintf("Server: %v put func as leader: key: %v, reqId:%v, index:%v", kv.me, args.Key, args.ReqId, index)

	timer := time.After(time.Duration(waitCommitTimeout) * time.Millisecond)
	for {
		select {
		case <-timer:
			reply.Err = ErrCommitTimeout
			DPrintf("Server: %v put timeout waiting commit", kv.me)
			return
		case res := <-resChan:
			reply.Err = res.Err
			reply.Index = res.Index
			Assert(res.Err == OK || res.Err == ErrDupRequest, "PutAppend: res.Err")
			return
		case <-time.After(time.Duration(checkLeaderInterval) * time.Millisecond):
			curTerm, curIsLeader := kv.rf.GetState()
			if curTerm != term || !curIsLeader {
				reply.Err = ErrCommitFail
				return
			}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.opChans = make(map[ReqIdentity]chan Result)
	kv.opHistory = make(map[ReqIdentity]Result)
	DPrintf("KVServer %v restart", me)

	// You may need initialization code here.
	go kv.readApply()

	return kv
}
