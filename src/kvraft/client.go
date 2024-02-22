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
	req := GetArgs{Key: key}
	reply := GetReply{}
	for {
		ck.mu.Lock()
		knownLeader := ck.knownLeader
		ck.mu.Unlock()
		for i := 0; i < len(ck.servers); i++ {
			f := ck.servers[(i+knownLeader)%len(ck.servers)].Call("KVServer.Get", &req, &reply)
			if f && reply.Err == OK {
				DPrintf("Get success. key:%v info on server %v: %v", key, i, reply)
				if i != 0 {
					ck.mu.Lock()
					ck.knownLeader = (i + knownLeader) % len(ck.servers)
					ck.mu.Unlock()
				}
				return reply.Value
			}
			DPrintf("Get fail. key:%v info on server %v: %v", key, i, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// You will have to modify this function.
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
	// You will have to modify this function.
	req := PutAppendArgs{Key: key, Value: value, Op: op}
	reply := PutAppendReply{}
	for {
		ck.mu.Lock()
		knownLeader := ck.knownLeader
		ck.mu.Unlock()
		for i := 0; i < len(ck.servers); i++ {
			f := ck.servers[(i+knownLeader)%len(ck.servers)].Call("KVServer.PutAppend", &req, &reply)
			if f && reply.Err == OK {
				DPrintf("Put success. info %+v on server %v: %v", req, i, reply)
				if i != 0 {
					ck.mu.Lock()
					ck.knownLeader = (i + knownLeader) % len(ck.servers)
					ck.mu.Unlock()
				}
				return
			}
			DPrintf("Put fail. info %+v on server %v: %v", req, i, reply)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
