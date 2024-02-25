package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	knownLeader int
	id          int64
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
	ck.knownLeader = 0
	ck.id = nrand()
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
func (ck *Clerk) Get(key string) string {
	res, _ := ck.GetTmp(key)
	// fmt.Printf("CLIENT: id %v return get key:%v, value:%v, index:%v\n", ck.id, key, res, index)
	return res
}
func (ck *Clerk) GetTmp(key string) (string, int) {

	baseReq := GetArgs{Key: key, ReqId: ReqIdentity{UUID: newUUID()}}
	baseReply := GetReply{}
	// DPrintf("CLIENT: clerk call get key:%v, reqId:%+v", key, baseReq.ReqId)

	for { // 无限尝试
		ck.mu.Lock()
		knownLeader := ck.knownLeader
		ck.mu.Unlock()

		i := 0
		for i < len(ck.servers) {
			req, reply := baseReq, baseReply
			curServerId := (i + knownLeader) % len(ck.servers)
			f := ck.servers[curServerId].Call("KVServer.Get", &req, &reply)
			if !f {
				i++
				continue
			}

			switch reply.Err {
			case OK, ErrDupRequest:
				if i != 0 {
					ck.mu.Lock()
					ck.knownLeader = curServerId
					ck.mu.Unlock()
				}
				// DPrintf("CLIENT: Get success. key:%v info on server %v: %+v", key, curServerId, reply)
				return reply.Value, reply.Index
			case ErrNotRunning, ErrWrongLeader:
			case ErrNoKey:
				return "", -1
			case ErrCommitFail, ErrCommitTimeout:
				// DPrintf("CLIENT: Get fail, commitfail or timeout. key:%v info on server %v: %+v", key, curServerId, reply.Err)
			}
			// case ErrCommitTimeout:
			// 	// maybe 还是Leader，也可能是网络分区了，暂时重试其他的服务器

			i++

		}
		time.Sleep(10 * time.Millisecond)
	}

	return "", -1
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
	ck.PutAppendTmp(key, value, op)
	// fmt.Printf("CLIENT: id %v return put key:%v, value:%v, index:%v\n", ck.id, key, value, index)
}
func (ck *Clerk) PutAppendTmp(key string, value string, op string) int {
	// You will have to modify this function.
	// var req PutAppendArgs
	// var reply PutAppendReply
	baseReq := PutAppendArgs{Key: key, Value: value, Op: op, ReqId: ReqIdentity{UUID: newUUID()}}
	baseReply := PutAppendReply{}

	for { // 无限尝试
		ck.mu.Lock()
		knownLeader := ck.knownLeader
		ck.mu.Unlock()

		i := 0
		for i < len(ck.servers) {
			req, reply := baseReq, baseReply
			curServerId := (i + knownLeader) % len(ck.servers)
			f := ck.servers[curServerId].Call("KVServer.PutAppend", &req, &reply)
			if !f {
				i++
				continue
			}

			// if op == "Put" {
			// 	DPrintf("CLIENT: issue put key:%v value:%v on server %v: %+v, reqId:%v", key, value, curServerId, reply, req.ReqId)
			// } else {
			// 	DPrintf("CLIENT: issue append key:%v value:%v on server %v: %+v, reqId:%v", key, value, curServerId, reply, req.ReqId)
			// }

			switch reply.Err {
			case OK, ErrDupRequest:
				if i != 0 {
					ck.mu.Lock()
					ck.knownLeader = curServerId
					ck.mu.Unlock()
				}
				// DPrintf("CLIENT: Put success. key:%v info on server %v: %+v", key, curServerId, reply)
				return reply.Index
			case ErrNotRunning, ErrWrongLeader:
			case ErrCommitFail, ErrCommitTimeout:
				// DPrintf("CLIENT: Put fail: commitfail or timeout. key:%v info on server %v: %+v", key, curServerId, reply.Err)
			default:
				Assert(false, "PutAppend fail, unknown error")
			}
			// case ErrCommitTimeout:
			// 	// maybe 还是Leader，也可能是网络分区了，暂时重试其他的服务器

			i++

			// DPrintf("Get fail. key:%v info on server %v: %v", key, i, reply)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
