package raft

import (
	"math/rand"
	"time"

	debuger "6.5840/helper"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debuger.DPrintf("pid = %v, candidate = %v, args.Term = %v, rf.term = %v\n", rf.me, args.CandidateId, args.Term, rf.term)

	needPersist := false

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
	} else {
		// 不是你比我大,我就同意你.有可能脑裂,一边一直选举term更大.
		if args.Term > rf.term {
			if rf.status == Leader {
				rf.commitCond.Signal()
				rf.appendCond.Broadcast()
			}
			rf.term = args.Term
			rf.status = Follower
			rf.voted_id = -1
		}
		if rf.status == Follower && rf.voted_id == -1 {
			// 检查日志是否更新，否则不同意
			local_idx := rf.getLastLogIndex()
			local_term := rf.getLastLogTerm()
			if args.LastLogTerm > local_term || (args.LastLogTerm == local_term && args.LastLogIdx >= local_idx) {
				rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
				rf.voted_id = args.CandidateId
				reply.VoteGranted = true
				needPersist = true
			} else {
				reply.VoteGranted = false
			}
			debuger.DPrintf("pid = %v, vote for %v return %v, local_idex = %v, local_term = %v, args.LastLogIdx = %v, args.LastLogTerm = %v\n", rf.me, args.CandidateId, reply.VoteGranted, local_idx, local_term, args.LastLogIdx, args.LastLogTerm)
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.term
	}

	if needPersist {
		// 返回前写入，防止多次投票
		rf.persist()
	}
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	if rf.status != Candidate {
		rf.mu.Unlock()
		return false
	}

	// send the RequestVote RPCs in parallel
	r := 150 + (rand.Int63() % 150)
	timer := time.After(time.Duration(r) * time.Millisecond)
	debuger.DPrintf("pid = %v, r = %v\n", rf.me, r)

	rp_chan := make(chan RequestVoteReply, len(rf.peers)-1)
	args := RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
		LastLogIdx:  rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			f := rf.sendRequestVote(i, &args, &reply) // 有可能是false，rpc失败直接不返回
			if f {
				rp_chan <- reply
			}
		}(i)
	}
	rf.mu.Unlock()

	// receive the RequestVote RPCs
	tickets := 0
	isTimeout := false

	for !isTimeout {
		select {
		case <-timer:
			debuger.DPrintf("pid = %v, timeout\n", rf.me)
			isTimeout = true
		case reply := <-rp_chan:
			rf.mu.Lock()
			if rf.status != Candidate {
				rf.mu.Unlock()
				return false
			}

			if reply.VoteGranted {
				tickets++
				if tickets >= len(rf.peers)/2 {
					rf.status = Leader
					rf.mu.Unlock()
					debuger.DPrintf("pid = %v becomes leader, has logs = %v\n", rf.me, rf.log)
					return true
				}
			} else {
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.status = Follower
					rf.voted_id = -1
					rf.mu.Unlock()
					return false
				}
			}
			rf.mu.Unlock()
		}
	}

	return false
}

func (rf *Raft) CandidateTick() {
	for !rf.killed() {
		if rf.startElection() {
			// 选举成功
			return
		}

		rf.mu.Lock()
		if rf.status != Candidate { // 没成功，变成了follower
			rf.mu.Unlock()
			return
		}
		rf.term += 1
		rf.voted_id = rf.me
		rf.mu.Unlock()
		debuger.DPrintf("pid = %v elect failes\n", rf.me)
	}
}
