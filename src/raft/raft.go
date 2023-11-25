package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	debuger "6.5840/helper"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftStatus int

const (
	Follower RaftStatus = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term       int
	status     RaftStatus
	elect_lock sync.Mutex
	leaderId   int
	voted_id   int

	alive_time int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.elect_lock.Lock()
	defer rf.elect_lock.Unlock()
	return rf.term, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
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
	rf.elect_lock.Lock()
	defer rf.elect_lock.Unlock()

	debuger.DPrintf("pid = %v, candidate = %v, args.Term = %v, rf.term = %v\n", rf.me, args.CandidateId, args.Term, rf.term)

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.VoteGranted = false
	} else if args.Term > rf.term {
		// 直接变成小弟
		rf.term = args.Term
		rf.status = Follower
		rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
		rf.voted_id = args.CandidateId

		reply.VoteGranted = true
		reply.Term = rf.term
		debuger.DPrintf("pid = %v, vote for %v\n", rf.me, args.CandidateId)
	} else {
		// 同term，先到先得
		debuger.DPrintf("term equal: pid = %v, args.CandidateId = %v, rf.voted_id = %v\n", rf.me, args.CandidateId, rf.voted_id)
		if rf.status == Follower && rf.voted_id == -1 {
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
			rf.voted_id = args.CandidateId

			reply.Term = rf.term
			reply.VoteGranted = true
			debuger.DPrintf("pid = %v, vote for %v\n", rf.me, args.CandidateId)
		} else {
			reply.Term = rf.term
			reply.VoteGranted = false
		}
	}
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	Entrys   []byte
}
type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// 如果entrys为空，就是心跳
	if len(args.Entrys) == 0 {
		rf.elect_lock.Lock()
		defer rf.elect_lock.Unlock()
		if args.Term < rf.term {
			reply.Term = rf.term
			reply.Success = false
		} else {
			rf.leaderId = args.LeaderId
			rf.status = Follower
			if rf.term < args.Term {
				rf.term = args.Term
			}
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
			reply.Success = true
			debuger.DPrintf("pid = %v, become follower, alive_time = %v\n", rf.me, rf.alive_time)
		}
		debuger.DPrintf("pid = %v, receive heartbeat, leadId = %v, args.term = %v, rf.term = %v, alive_time = %v, cur_time = %v \n", rf.me, args.LeaderId, args.Term, rf.term, rf.alive_time, time.Now().UnixNano()/int64(time.Millisecond))
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntry RPC
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	heartBeatInterval int64 = 100
	electInterval     int64 = 150
)

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.elect_lock.Lock()
		s := rf.status
		rf.elect_lock.Unlock()
		switch s {
		case Follower:
			r := electInterval
			for {
				t := time.Now().UnixNano() / int64(time.Millisecond)
				rf.elect_lock.Lock()
				a := rf.alive_time
				rf.elect_lock.Unlock()
				debuger.DPrintf("pid = %v, t = %v, a = %v, r = %v, interval = %v\n", rf.me, t, a, r, t-a)
				if t-a < r {
					// 时间没到，就sleep一段时间
					time.Sleep(time.Duration((r - (t - a))) * time.Millisecond)
				} else {
					rf.elect_lock.Lock()
					rf.status = Candidate
					rf.term++
					rf.voted_id = rf.me
					rf.elect_lock.Unlock()
					debuger.DPrintf("pid = %v become candidate\n", rf.me)
					break
				}
			}

		case Candidate:
			if rf.startElection() {
				break
			}

			// judge
			rf.elect_lock.Lock()
			if rf.status == Follower {
				// 变成小弟后更新alive时间，不然选举失败后起点靠后容易超时
				rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
				rf.elect_lock.Unlock()
				break
			}
			debuger.DPrintf("pid = %v elect failes\n", rf.me)
			rf.term += 1
			rf.voted_id = rf.me
			rf.elect_lock.Unlock()

		case Leader:
			// 周期发心跳
			rf.sendHeartBeat()
			time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)

		default:
			// do nothing
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElection() bool {
	// 群发选票，接收
	r := 150 + (rand.Int63() % 150)
	timer := time.After(time.Duration(r) * time.Millisecond)
	debuger.DPrintf("pid = %v, r = %v\n", rf.me, r)

	rf.elect_lock.Lock()
	cur_term := rf.term
	rf.elect_lock.Unlock()

	rp_chan := make(chan RequestVoteReply, len(rf.peers)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{
			Term:        cur_term,
			CandidateId: rf.me,
		}
		go func(i int) {
			reply := RequestVoteReply{}
			f := rf.sendRequestVote(i, &args, &reply) // 有可能是false，rpc失败直接不返回
			if f {
				rp_chan <- reply
			}
		}(i)
	}

	tickets := 0
	isTimeout := false

	for !isTimeout { // isFollower的话直接转换
		rf.elect_lock.Lock()
		if rf.status != Candidate { // 收到更大的term 立即变 follower, 收到票数过半 -> leader，都立即返回
			rf.elect_lock.Unlock()
			break
		}
		rf.elect_lock.Unlock()

		select {
		case <-timer:
			debuger.DPrintf("pid = %v, timeout\n", rf.me)
			isTimeout = true
		case reply := <-rp_chan:
			if reply.VoteGranted {
				tickets++
				if tickets >= len(rf.peers)/2 {
					rf.elect_lock.Lock()
					rf.status = Leader
					rf.elect_lock.Unlock()
					debuger.DPrintf("pid = %v become leader\n", rf.me)
				}
			} else {
				// check一下：减少后续判断
				rf.elect_lock.Lock()
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.status = Follower
					rf.voted_id = -1
				}
				rf.elect_lock.Unlock()
			}
		}
	}

	rf.elect_lock.Lock()
	defer rf.elect_lock.Unlock()
	return rf.status == Leader
}

func (rf *Raft) sendHeartBeat() {
	debuger.DPrintf("pid = %v, send heartbeat\n", rf.me)
	rf.elect_lock.Lock()
	cur_term := rf.term
	rf.elect_lock.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntryArgs{}
		args.Term = cur_term
		args.LeaderId = rf.me
		args.Entrys = []byte{}
		go func(i int) {
			reply := AppendEntryReply{}
			f := rf.sendAppendEntry(i, &args, &reply)
			debuger.DPrintf("pid = %v, send heartbeat to %v, f = %v\n", rf.me, i, f)
		}(i)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		term:       0,
		status:     Follower,
		alive_time: time.Now().UnixNano() / int64(time.Millisecond),
		voted_id:   -1,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
