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

	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	debuger "6.5840/helper"
	"6.5840/labgob"
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
	leaderId   int
	voted_id   int
	alive_time int64

	// 同时加锁顺序client_notifer条件变量先，elect先，rep后，防止死锁

	// for all servers
	log           []LogEntry
	commitedIndex int
	lastApplied   int
	applyCh       chan ApplyMsg

	// for leader
	nextIndex  []int
	matchIndex []int
	// log_notifer          chan struct{}
	client_notifier_cond *sync.Cond
	client_notifier_lock sync.Mutex

	emptyEntryNum int
}

type RaftPersist struct {
	// 需要持久化的raft变量
	Me       int
	Term     int
	Status   RaftStatus
	Voted_id int

	Log []LogEntry
	// commitedIndex int // 这个可以可无感觉
	LastApplied int

	// for leader
	NextIndex  []int
	MatchIndex []int
}

func (rf *Raft) getPersistField() RaftPersist {
	return RaftPersist{
		Me:       rf.me,
		Term:     rf.term,
		Status:   rf.status,
		Voted_id: rf.voted_id,

		Log:         rf.log,
		LastApplied: rf.lastApplied,

		NextIndex:  rf.nextIndex,
		MatchIndex: rf.matchIndex,
	}
}

func (rf *Raft) setPersistField(r *RaftPersist) {
	rf.me = r.Me
	rf.term = r.Term
	rf.status = r.Status
	rf.voted_id = r.Voted_id

	rf.log = r.Log
	rf.lastApplied = r.LastApplied

	rf.nextIndex = r.NextIndex
	rf.matchIndex = r.MatchIndex
}

type LogEntry struct {
	Command interface{} // 万能接口
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	// 存储当前raft状态，用于机器重启时回复
	// var buffer bytes.Buffer
	// encoder := labgob.NewEncoder(&buffer)
	// encoder.Encode(rf.getPersistField())
	// data := buffer.Bytes()
	// // debuger.DPrintf("pid = %v, persist, rf = %v, len = %v\n", rf.me, rf.getPersistField(), len(data))
	// rf.persister.Save(data, nil)
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

	// 读取持久化的状态
	var p RaftPersist
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	decoder.Decode(&p)
	rf.setPersistField(&p)
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
			rf.term = args.Term
			rf.status = Follower
			rf.voted_id = -1
			needPersist = true
		}
		if rf.status == Follower && rf.voted_id == -1 {
			// 检查日志是否更新，否则不同意
			if len(rf.log) == 0 {
				rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
				rf.voted_id = args.CandidateId
				reply.VoteGranted = true
				needPersist = true
			} else {
				debuger.DPrintf("pid = %v, args.LastLogTerm = %v, args.LastLogIdx = %v\n", rf.me, args.LastLogTerm, args.LastLogIdx)
				debuger.DPrintf("pid = %v, my.LastLogTerm = %v, len(rf.log) = %v\n", rf.me, rf.log[len(rf.log)-1].Term, len(rf.log))
				if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
					(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIdx >= len(rf.log)) {
					rf.voted_id = args.CandidateId
					reply.VoteGranted = true
					rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
					needPersist = true
				} else {
					reply.VoteGranted = false
				}
			}
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.term
		debuger.DPrintf("pid = %v, vote for %v, self log = %v, args = %v, reply = %v\n", rf.me, args.CandidateId, rf.log, args, reply)
	}

	if needPersist {
		// 返回前写入，防止多次投票
		rf.persist()
	}

}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	// 用于匹配
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	// 用于帮助client确认可以commit
	LeaderCommit int
}
type AppendEntryReply struct {
	Term    int
	Success bool

	// 优化nextIndex的交互
	NextIndex int
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	needPersist := false
	if len(args.Entries) == 0 { // 心跳
		if args.Term < rf.term {
			reply.Term = rf.term
			reply.Success = false
		} else {
			rf.leaderId = args.LeaderId
			rf.status = Follower
			rf.voted_id = -1
			if rf.term < args.Term {
				rf.term = args.Term
			}
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
			needPersist = true

			reply.Success = true
			reply.Term = args.Term

			// 当前机器log可能不一致，leader的commitindex不work。
			// 找到最后一条leader同步后的最新log，再进行更新
			lastidx := 0
			for i := len(rf.log); i >= 1; i-- {
				if rf.log[i-1].Term == args.Term {
					lastidx = i
					break
				}
			}
			if lastidx != 0 {
				if args.LeaderCommit > rf.commitedIndex {
					if args.LeaderCommit > lastidx {
						rf.commitedIndex = lastidx
					} else {
						rf.commitedIndex = args.LeaderCommit
					}
				}
			}
			debuger.DPrintf("pid = %v, lastidx = %v, leaderCommit = %v, commitedIndex = %v\n", rf.me, lastidx, args.LeaderCommit, rf.commitedIndex)
			debuger.DPrintf("pid = %v, update commitIndex = %v\n", rf.me, rf.commitedIndex)
		}

		debuger.DPrintf("pid = %v, receive heartbeat, leadId = %v, args.term = %v, rf.term = %v, alive_time = %v, cur_time = %v \n", rf.me, args.LeaderId, args.Term, rf.term, rf.alive_time, time.Now().UnixNano()/int64(time.Millisecond))
	} else { // append
		if args.Term < rf.term {
			reply.Term = rf.term
			reply.Success = false
		} else {
			if args.Term > rf.term { // 我可能是个leader，变成follower直接开始work
				rf.term = args.Term
				rf.status = Follower
				rf.voted_id = -1
				needPersist = true
			}
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)

			// 先匹配PrevLogIndex和PrevLogTerm
			if args.PrevLogIndex == 0 {
				reply.Success = true
				reply.Term = args.Term
			} else if args.PrevLogIndex <= len(rf.log) {
				log_term := rf.log[args.PrevLogIndex-1].Term
				if log_term == args.PrevLogTerm {
					reply.Success = true
					reply.Term = args.Term
				} else {
					reply.Success = false
					reply.Term = args.Term
				}
			} else {
				reply.Success = false
				reply.Term = args.Term
			}

			debuger.DPrintf("pid = %v append log status, PrevLogIndex = %v, PrevLogTerm = %v, reply = %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply)
			if reply.Success {
				debuger.DPrintf("pid = %v, append entrys = %v, cur entrys = %v\n", rf.me, args.Entries, rf.log)
				// 匹配成功后，删除后面其他任期的（其他leader时期没复制成功）
				for i := 0; i < len(args.Entries); i++ {
					if args.PrevLogIndex+i >= len(rf.log) {
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
					if args.Entries[i].Term != rf.log[args.PrevLogIndex+i].Term {
						rf.log = rf.log[:args.PrevLogIndex+i]
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
				}

				debuger.DPrintf("pid = %v, lastest.Entries = %v\n", rf.me, rf.log)

				// 更新committedIndex
				if args.LeaderCommit > rf.commitedIndex {
					if args.LeaderCommit > len(rf.log) {
						rf.commitedIndex = len(rf.log)
					} else {
						rf.commitedIndex = args.LeaderCommit
					}
				}
				needPersist = true
			} else {
				// 优化返回nextIndex，发commitIndex的位置，可以允许多发
				reply.NextIndex = rf.commitedIndex + 1
			}

		}
		debuger.DPrintf("pid = %v receive append end, leadId = %v, args.term = %v, rf.term = %v, alive_time = %v, cur_time = %v \n", rf.me, args.LeaderId, args.Term, rf.term, rf.alive_time, time.Now().UnixNano()/int64(time.Millisecond))
	}
	if needPersist {
		rf.persist()
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
	// Your code here (2B).
	index := -1
	term := -1
	isLeader := true

	debuger.DPrintf("pid = %v, receive client command\n", rf.me)

	rf.mu.Lock()
	if rf.status != Leader {
		rf.mu.Unlock()
		debuger.DPrintf("pid = %v, not leader\n", rf.me)
		return index, term, false
	}

	E := LogEntry{
		Command: command,
		Term:    rf.term,
	}
	valid_cnt := 0
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Command != nil {
			valid_cnt++
		}
	}
	term, index = rf.term, valid_cnt+1
	rf.log = append(rf.log, E)
	if isLeader {
		debuger.DPrintf("pid = %v handle client command = %v, index = %v, term = %v, logs = %v\n", rf.me, command, index, term, rf.log)
	}

	rf.mu.Unlock()

	return index, term, isLeader // 要不要考虑持久化呢。
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
	heartBeatInterval  int64 = 100
	electInterval      int64 = 300
	rpcErrorInterval   int64 = 10
	selectIdleInterval int64 = 10
)

func (rf *Raft) appendEmptyForCatch() {
	// 发一个空的，帮助其他匹配
	rf.log = append(rf.log, LogEntry{
		Command: nil,
		Term:    rf.term,
	})
}

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return
	}

	go rf.sendHeartBeat(rf.term)

	go rf.checkCommit(rf.term)

	// 选举后初始化nextIndex和matchIndex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}
	// rf.appendEmptyForCatch()
}

func (rf *Raft) checkStatus(s RaftStatus) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status == s
}

func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		s := rf.status
		rf.mu.Unlock()

		switch s {
		case Follower:
			r := electInterval
			for !rf.killed() {
				t := time.Now().UnixNano() / int64(time.Millisecond)
				rf.mu.Lock()
				a := rf.alive_time
				rf.mu.Unlock()
				debuger.DPrintf("pid = %v, t = %v, a = %v, r = %v, interval = %v\n", rf.me, t, a, r, t-a)
				if t-a < r {
					// cpu idle
					time.Sleep(time.Duration((r - (t - a))) * time.Millisecond)
				} else {
					rf.mu.Lock()
					rf.status = Candidate
					rf.term++
					rf.voted_id = rf.me
					debuger.DPrintf("pid = %v become candidate\n", rf.me)
					rf.persist()
					rf.mu.Unlock()
					break
				}
			}

		case Candidate:
			if rf.startElection() {
				break
			}

			// judge
			if !rf.checkStatus(Candidate) {
				break
			}

			rf.mu.Lock()
			rf.term += 1
			rf.voted_id = rf.me
			rf.persist()
			rf.mu.Unlock()

			debuger.DPrintf("pid = %v elect failes\n", rf.me)

		case Leader:
			rf.initLeader() // 心跳，检查commit，初始化NextIndex和MatchIndex

			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				break
			}
			go rf.checkAppend(rf.term) // 为了简化实现，暂不用条件变量，循环检查
			rf.mu.Unlock()

			for !rf.killed() && rf.checkStatus(Leader) {
				time.Sleep(time.Duration(selectIdleInterval) * time.Millisecond)
			}

		default:
			// do nothing

		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkApply() {
	// 检查是否有新的entry可以commit
	for !rf.killed() {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitedIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-1].Command,
				CommandIndex: rf.lastApplied,
			}

			if applyMsg.Command != nil {
				applyMsg.CommandIndex -= rf.emptyEntryNum
				rf.applyCh <- applyMsg
			} else {
				rf.emptyEntryNum++
			}
			debuger.DPrintf("pid = %v, applyMsg = %v, appliedlen = %v, hadcommitted = %v\n", rf.me, applyMsg, rf.lastApplied, rf.commitedIndex)
			rf.mu.Unlock()

			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			time.Sleep(time.Duration(selectIdleInterval) * time.Millisecond)
		}
	}
}

func (rf *Raft) checkCommit(leader_term int) {
	// 当前任期，检查并commit
	for !rf.killed() {
		// <-commit_check_notifer // 用条件变量好像比较合适，因为信号器不用等待一定发送成功
		time.Sleep(time.Duration(selectIdleInterval*3) * time.Millisecond) // 先简化一下吧，不用条件变量了。
		// 如何条件变量：阻塞等待有entrys同步成功检查是否多数派，条件是leader任期，commitIndex小于logs大小
		// debuger.DPrintf("pid = %v, starts to commit check\n", rf.me)

		// 先检查是不是leader
		rf.mu.Lock()
		if rf.status != Leader || rf.term != leader_term {
			rf.mu.Unlock()
			break
		}

		// 检查是否满足提交的条件
		debuger.DPrintf("pid = %v, term = %v, starts to commit check, commitedIndex = %v, log = %v\n", rf.me, rf.term, rf.commitedIndex, rf.log)
		p := []int{rf.me}
		for i := len(rf.log); i > rf.commitedIndex; i-- {
			// 多数客户端满足则提交
			if rf.log[i-1].Term == leader_term {
				cnt := 1
				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						cnt++
						p = append(p, j)
					}
				}
				if cnt >= len(rf.peers)/2+1 {
					rf.commitedIndex = i
					// rf.persist()

					debuger.DPrintf("pid = %v, commit entry = %v, index = %v\n", rf.me, rf.log[i-1], rf.commitedIndex)
					break
				}
			}
		}
		rf.mu.Unlock()
		debuger.DPrintf("pid = %v, commit check end, commitedIndex = %v, len(log) = %v, peers = %v\n", rf.me, rf.commitedIndex, len(rf.log), p)

	}
}

func (rf *Raft) checkAppend(leader_term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.appendPeer(leader_term, i)
	}
}

func (rf *Raft) appendPeer(leader_term int, peer_id int) {
	for {
		rf.mu.Lock()

		if rf.status != Leader || rf.term != leader_term {
			debuger.DPrintf("pid = %v, is not leader when appending to %v\n", rf.me, peer_id)
			rf.mu.Unlock()
			break
		}

		debuger.DPrintf("pid = %v start to append to %v, nextIndex = %v, len rf.log = %v, matchIndex = %v\n", rf.me, peer_id, rf.nextIndex[peer_id], len(rf.log), rf.matchIndex[peer_id])

		if rf.nextIndex[peer_id] <= len(rf.log) {
			prevLogIndex := rf.nextIndex[peer_id] - 1
			args := AppendEntryArgs{
				Term:         leader_term,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				Entries:      rf.log[prevLogIndex:],
				LeaderCommit: rf.commitedIndex,
			}
			if prevLogIndex != 0 {
				args.PrevLogTerm = rf.log[prevLogIndex-1].Term
			}
			rf.mu.Unlock()

			reply := AppendEntryReply{}
			f := rf.sendAppendEntry(peer_id, &args, &reply)
			if !f {
				continue
			} else {
				debuger.DPrintf("pid = %v, append to %v, reply = %v, args = %+v\n", rf.me, peer_id, reply, args)
			}

			rf.mu.Lock()
			if rf.status != Leader || rf.term != leader_term {
				rf.mu.Unlock()
				break
			}

			if !reply.Success {
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.status = Follower
					rf.voted_id = -1
					rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
				} else {
					rf.nextIndex[peer_id] = reply.NextIndex
					debuger.DPrintf("pid = %v, append to %v not match, matchIndex = %v, nextIndex = %v, reply = %+v\n", rf.me, peer_id, rf.matchIndex[peer_id], rf.nextIndex[peer_id], reply)
				}
			} else {
				// 更新nextIndex和matchIndex，和发送的保持一致
				rf.matchIndex[peer_id] = rf.nextIndex[peer_id] + len(args.Entries) - 1
				rf.nextIndex[peer_id] = len(rf.log) + 1
				debuger.DPrintf("pid = %v, append to %v success, matchIndex = %v, nextIndex = %v\n", rf.me, peer_id, rf.matchIndex[peer_id], rf.nextIndex[peer_id])
			}
			// rf.persist()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			time.Sleep(time.Duration(selectIdleInterval) * time.Millisecond) // 批量检查提交
		}
	}
}

func (rf *Raft) startElection() bool {
	// 群发选票，接收
	r := 150 + (rand.Int63() % 150)
	timer := time.After(time.Duration(r) * time.Millisecond)
	debuger.DPrintf("pid = %v, r = %v\n", rf.me, r)

	rf.mu.Lock()

	lastLogIndex := len(rf.log)
	lastLogTerm := 0
	if lastLogIndex != 0 {
		lastLogTerm = rf.log[lastLogIndex-1].Term
	}

	rp_chan := make(chan RequestVoteReply, len(rf.peers)-1)
	args := RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
		LastLogIdx:  lastLogIndex,
		LastLogTerm: lastLogTerm,
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

	tickets := 0
	isTimeout := false

	for !isTimeout && rf.checkStatus(Candidate) { // isFollower的话直接转换
		select {
		case <-timer:
			debuger.DPrintf("pid = %v, timeout\n", rf.me)
			isTimeout = true
		case reply := <-rp_chan:
			rf.mu.Lock()
			needPersist := false
			if reply.VoteGranted {
				tickets++
				if tickets >= len(rf.peers)/2 {
					rf.status = Leader
					needPersist = true
					debuger.DPrintf("pid = %v become leader, has logs = %v\n", rf.me, rf.log)
				}
			} else {
				// check一下：减少后续判断
				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.status = Follower
					rf.voted_id = -1
					needPersist = true
				}
			}
			if needPersist {
				rf.persist()
			}
			rf.mu.Unlock()
		default:
			// do nothing
			time.Sleep(time.Duration(selectIdleInterval) * time.Millisecond)
		}

	}

	return rf.checkStatus(Leader)
}

func (rf *Raft) sendHeartBeat(leader_term int) {
	for {
		rf.mu.Lock()
		if rf.status != Leader || rf.term != leader_term {
			rf.mu.Unlock()
			break
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				if rf.status != Leader || rf.term != leader_term {
					rf.mu.Unlock()
					return
				}
				args := AppendEntryArgs{}
				args.Term = rf.term
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitedIndex
				// args.PrevLogIndex = rf.matchIndex[i] // 用于follower提交
				rf.mu.Unlock()

				reply := AppendEntryReply{}
				f := rf.sendAppendEntry(i, &args, &reply)
				rf.mu.Lock()
				if f {
					if reply.Term > rf.term {
						rf.term = reply.Term
						rf.status = Follower
						rf.voted_id = -1
						rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}(i)
		}
		rf.mu.Unlock()

		// debuger.DPrintf("pid = %v, send heartbeat, time = %v\n", rf.me, time.Now().UnixNano()/int64(time.Millisecond))

		time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
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
		peers:         peers,
		persister:     persister,
		me:            me,
		term:          0,
		status:        Follower,
		alive_time:    time.Now().UnixNano() / int64(time.Millisecond),
		voted_id:      -1,
		commitedIndex: 0,
		lastApplied:   0,
		// log_notifer:   make(chan struct{}, 100), // 多创建几个channel，提高性能，用于客户端通知有消息来
		applyCh:       applyCh,
		emptyEntryNum: 0,
	}
	rf.client_notifier_cond = sync.NewCond(&rf.client_notifier_lock)

	// Your initialization code here (2A, 2B, 2C).
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 检查提交
	go rf.checkApply()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
