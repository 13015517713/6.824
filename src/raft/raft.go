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
	alive_time int64 // 心跳时间

	// for all servers
	log           []LogEntry
	commitedIndex int
	lastApplied   int
	applyCh       chan ApplyMsg

	// 待执行队列
	applyLock sync.Mutex
	applyList []ApplyMsg

	// for leader
	nextIndex  []int
	matchIndex []int

	// cond notify checkcommit
	commitCond *sync.Cond
	needCheck  bool
	appendCond *sync.Cond
	applyCond  *sync.Cond

	// for snapshot
	lastIncludedIndex int
	lastIncludedTerm  int

	// isKilled ?
	live bool // kill之后设置live
}

type RaftPersist struct {
	// 需要持久化的raft变量
	Me       int
	Term     int
	Voted_id int

	Log []LogEntry

	// for snapshot
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (rf *Raft) getPersistField() RaftPersist {
	return RaftPersist{
		Me:       rf.me,
		Term:     rf.term,
		Voted_id: rf.voted_id,

		Log: rf.log,

		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
}

func (rf *Raft) setPersistField(r *RaftPersist) {
	rf.me = r.Me
	rf.term = r.Term
	rf.voted_id = r.Voted_id // 防止一个任期内多次投票

	rf.log = r.Log // 为了投票时的判定

	rf.lastIncludedIndex = r.LastIncludedIndex
	rf.lastIncludedTerm = r.LastIncludedTerm

}

type LogEntry struct {
	Command interface{} // 万能接口
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.status == Leader
}

// 写入snapshot时需要同时保存当前状态，故障回复
func (rf *Raft) persistSnap(snapshot []byte) {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(rf.getPersistField())
	data := buffer.Bytes()
	// debuger.DPrintf("pid = %v, persist, rf = %v, len = %v\n", rf.me, rf.getPersistField(), len(data))
	rf.persister.Save(data, snapshot)
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
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(rf.getPersistField())
	data := buffer.Bytes()
	// debuger.DPrintf("pid = %v, persist, rf = %v, len = %v\n", rf.me, rf.getPersistField(), len(data))
	if rf.persister.SnapshotSize() != 0 {
		rf.persister.Save(data, rf.persister.ReadSnapshot())
	} else {
		rf.persister.Save(data, nil)
	}
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
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitedIndex = rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// index之前的log都删掉(二分+链表，貌似跳表比较合适)，剩下的保留，把快照和当前状态都保留。
	debuger.DPrintf("pid = %v start Snapshot, index = %v, snapshot size = %v, lastApplied = %v\n", rf.me, index, len(snapshot), rf.lastApplied)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debuger.DPrintf("pid = %v local success Snapshot, index = %v, snapshot size = %v, lastApplied = %v\n", rf.me, index, len(snapshot), rf.lastApplied)
	if index <= rf.lastIncludedIndex {
		return
	}

	f, idx := rf.findInLogs(index)
	Assert(f, "snapshot error, not find index in logs")
	Assert(rf.log[idx].Index == index, "snapshot error, not find index in logs")
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[idx].Term
	rf.log = rf.log[idx+1:]
	debuger.DPrintf("pid = %v call Snapshot, index = %v, snapshot size = %v, lastApplied = %v\n", rf.me, index, len(snapshot), rf.lastApplied)
	rf.persistSnap(snapshot)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 加installRPC函数，leader发给其他
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 当nextindex已经在快照中就发，以保持同步
	// 也看作和appendEntry类似的方式，发完之后更新matchIndex
	rf.mu.Lock()

	debuger.DPrintf("pid = %v, receive install snapshot, args = %+v\n", rf.me, args)

	if args.Term < rf.term {
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.status = Follower
		rf.voted_id = -1
		rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
	}
	reply.Term = rf.term

	debuger.DPrintf("pid = %v, receive install snapshot, LastIncludeIndex = %v, rf.lastApplied = %v\n", rf.me, args.LastIncludedIndex, rf.lastApplied)
	if args.LastIncludedIndex <= rf.lastIncludedIndex || args.LastIncludedIndex < rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	f, idx := rf.findInLogs(args.LastIncludedIndex)
	if !f {
		rf.log = []LogEntry{}
		// 全用快照
	} else {
		rf.log = rf.log[idx+1:]
	}

	if rf.commitedIndex < args.LastIncludedIndex {
		rf.commitedIndex = args.LastIncludedIndex
	}
	rf.lastApplied = args.LastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persistSnap(args.Data)
	debuger.DPrintf("pid = %v, install snapshot, LastIncludedIndex = %v, lastApplied = %v", rf.me, rf.lastIncludedIndex, rf.lastApplied)

	rf.applyLock.Lock()
	rf.applyList = append(rf.applyList, ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	})
	rf.applyLock.Unlock()

	rf.mu.Unlock()
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
		if reply.VoteGranted {
			debuger.DPrintf("pid = %v, vote for %v, self log index= %v, reply = %v\n", rf.me, args.CandidateId, rf.getLastLogIndex(), reply)
		}
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

	// needPersist := false
	if len(args.Entries) == 0 { // 心跳
		if args.Term < rf.term {
			reply.Term = rf.term
			reply.Success = false
		} else {
			if rf.term < args.Term {
				rf.term = args.Term
				if rf.status == Leader {
					rf.commitCond.Signal()    // 降级也会唤醒
					rf.appendCond.Broadcast() // 降级也会唤醒
				}
			}
			rf.leaderId = args.LeaderId
			rf.status = Follower
			rf.voted_id = args.LeaderId // 防止同级在竞争的多次投票
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)

			reply.Success = true
			reply.Term = args.Term

			// 当前机器log可能不一致，leader的commitindex不work。
			// 找到最后一条leader同步后的最新log，再进行更新
			lastidx := 0
			for i := len(rf.log); i >= 1; i-- { // 这里也可以二分，term也是有序的
				if rf.log[i-1].Term == args.Term {
					lastidx = rf.log[i-1].Index
					break
				}
			}
			if lastidx != 0 {
				o_commit := rf.commitedIndex
				if args.LeaderCommit > rf.commitedIndex {
					if args.LeaderCommit > lastidx {
						rf.commitedIndex = lastidx
					} else {
						rf.commitedIndex = args.LeaderCommit
					}
				}
				if rf.commitedIndex > o_commit {
					rf.applyCond.Signal() // 唤醒检查应用
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
				if rf.status == Leader {
					rf.commitCond.Signal()    // 降级也会唤醒
					rf.appendCond.Broadcast() // 降级也会唤醒
				}
				rf.term = args.Term
				rf.status = Follower
				rf.voted_id = -1
			}
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)

			pre_match := -1
			reply.Term = args.Term
			// 先匹配PrevLogIndex和PrevLogTerm
			if args.PrevLogIndex == 0 {
				reply.Success = true
				pre_match = 0
			} else if args.PrevLogIndex < rf.lastIncludedIndex {
				Assert(false, "PrevLogIndex less than lastIncludedIndex in snapshot")
			} else if args.PrevLogIndex == rf.lastIncludedIndex {
				if args.PrevLogTerm == rf.lastIncludedTerm {
					reply.Success = true
					pre_match = 0
				} else {
					// 已提交的肯定是要匹配成功的
					Assert(false, "prevLogTerm not match in snapshot")
					reply.Success = false
				}
			} else {
				f, idx := rf.findInLogs(args.PrevLogIndex)
				if f {
					if rf.log[idx].Term == args.PrevLogTerm {
						reply.Success = true
						pre_match = idx + 1
					} else {
						reply.Success = false
					}
				} else {
					reply.Success = false
				}
			}

			debuger.DPrintf("pid = %v append log status, PrevLogIndex = %v, PrevLogTerm = %v, reply = %v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply)
			if reply.Success {
				// 匹配成功后，删除后面其他任期的（其他leader时期没复制成功
				for i := 0; i < len(args.Entries); i++ {
					if pre_match+i >= len(rf.log) {
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
					if args.Entries[i].Term != rf.log[pre_match+i].Term {
						rf.log = rf.log[:pre_match+i]
						rf.log = append(rf.log, args.Entries[i:]...)
						break
					}
				}

				debuger.DPrintf("pid = %v, lastest.Entries index = %v\n", rf.me, rf.getLastLogIndex())

				// 更新committedIndex
				latest_idx := rf.getLastLogIndex()
				if args.LeaderCommit > rf.commitedIndex {
					if args.LeaderCommit > latest_idx {
						rf.commitedIndex = latest_idx
					} else {
						rf.commitedIndex = args.LeaderCommit
					}
					rf.applyCond.Signal() // 唤醒检查应用
				}
				rf.persist()
				// needPersist = true
			} else {
				// 优化返回nextIndex，发commitIndex的位置，可以允许多发
				reply.NextIndex = rf.commitedIndex + 1
			}

		}
		debuger.DPrintf("pid = %v receive append end, leadId = %v, args.term = %v, rf.term = %v, alive_time = %v, cur_time = %v \n", rf.me, args.LeaderId, args.Term, rf.term, rf.alive_time, time.Now().UnixNano()/int64(time.Millisecond))
	}
	// if needPersist {
	// 	rf.persist()
	// }
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

// InstallSnapShot RPC
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	if rf.status != Leader {
		debuger.DPrintf("pid = %v, not leader\n", rf.me)
		return index, term, false
	}

	index = rf.getLastLogIndex() + 1
	term = rf.term
	E := LogEntry{
		Command: command,
		Term:    rf.term,
		Index:   index,
	}
	rf.log = append(rf.log, E)
	rf.persist()
	rf.appendCond.Broadcast() // 通知所有channel去发logs

	debuger.DPrintf("pid = %v, receive client command, entry = %+v\n", rf.me, E)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 唤醒检查
	rf.live = false
	rf.commitCond.Signal()
	rf.applyCond.Signal()
	rf.appendCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	heartBeatInterval  int64 = 100
	electInterval      int64 = 300
	rpcErrorInterval   int64 = 500 // 500ms不返回，rpc失败
	selectIdleInterval int64 = 10
	applyInterval      int64 = 500 // us, apply的速度是上限, tester的实现限制了apply的速度
)

func (rf *Raft) initLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return
	}

	go rf.sendHeartBeat(rf.term)

	go rf.checkAppend(rf.term)

	// 选举后初始化nextIndex和matchIndex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
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
				rf.mu.Lock()
				t := time.Now().UnixNano() / int64(time.Millisecond)
				a := rf.alive_time
				debuger.DPrintf("pid = %v, t = %v, a = %v, r = %v, interval = %v\n", rf.me, t, a, r, t-a)
				if t-a < r {
					// cpu idle
					rf.mu.Unlock()
					time.Sleep(time.Duration((r - (t - a))) * time.Millisecond)
				} else {
					rf.term++
					rf.status = Candidate
					rf.voted_id = rf.me
					rf.mu.Unlock()
					debuger.DPrintf("pid = %v become candidate\n", rf.me)
					break
				}
			}

		case Candidate:
			for !rf.killed() {
				if rf.startElection() {
					// 选举成功
					break
				}

				rf.mu.Lock()
				if rf.status != Candidate { // 没成功，变成了follower
					rf.mu.Unlock()
					break
				}
				rf.term += 1
				rf.voted_id = rf.me
				rf.mu.Unlock()
				debuger.DPrintf("pid = %v elect failes\n", rf.me)
			}

		case Leader:
			rf.initLeader() // 心跳，检查commit，初始化NextIndex和MatchIndex

			// 当append成功后，唤醒检查
			// 当leader降级后，唤醒检查
			rf.mu.Lock()
			for rf.live && rf.status == Leader { // 检查过killed，结果突然kill了，没接到信号。会一直等在这里。用mutex保护一个live
				if rf.needCheck {
					rf.checkCommitOnce(rf.term)
					rf.needCheck = false
				}
				rf.commitCond.Wait()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) submitApply() {
	for !rf.killed() {
		rf.applyLock.Lock()
		for len(rf.applyList) == 0 {
			rf.applyLock.Unlock()
			time.Sleep(time.Duration(applyInterval) * time.Microsecond)
			rf.applyLock.Lock()
		}
		applyMsg := rf.applyList[0]
		rf.applyList = rf.applyList[1:]
		rf.applyLock.Unlock()

		rf.applyCh <- applyMsg
	}
}

func (rf *Raft) checkApply() {
	// 定义一个channel
	for {
		rf.mu.Lock()

		for rf.live && rf.lastApplied >= rf.commitedIndex {
			rf.applyCond.Wait()
		}
		if !rf.live {
			rf.mu.Unlock()
			break
		}

		if rf.lastApplied < rf.commitedIndex {
			rf.lastApplied++

			f, idx := rf.findInLogs(rf.lastApplied)
			debuger.DPrintf("pid = %v, ready to apply index = %v, loglen = %v", rf.me, rf.lastApplied, len(rf.log))

			Assert(f, "checkApplyOnce search index error")
			Assert(rf.log[idx].Index == rf.lastApplied, "checkApplyOnce search index error")

			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[idx].Command,
				CommandIndex: rf.lastApplied,
			}
			debuger.DPrintf("pid = %v, ready to apply success index = %v, loglen = %v", rf.me, rf.lastApplied, len(rf.log))

			rf.applyLock.Lock()
			rf.applyList = append(rf.applyList, applyMsg)
			rf.applyLock.Unlock()

			debuger.DPrintf("pid = %v, applyMsg = %v, appliedlen = %v, hadcommitted = %v\n", rf.me, applyMsg, rf.lastApplied, rf.commitedIndex)
		}
		rf.mu.Unlock() // 一直占用锁，可能导致重新选举
		time.Sleep(time.Duration(applyInterval) * time.Microsecond)
	}
}

func (rf *Raft) checkCommitOnce(leader_term int) {
	for i := len(rf.log); i > 0; i-- {
		idx, term := rf.log[i-1].Index, rf.log[i-1].Term
		if idx <= rf.commitedIndex || term < leader_term {
			break
		}
		if term == leader_term {
			cnt := 1
			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= idx {
					cnt++
				}
			}
			if cnt >= len(rf.peers)/2+1 {
				rf.commitedIndex = idx
				rf.applyCond.Signal() // 唤醒检查
				break
			}
		}
	}
	debuger.DPrintf("pid = %v, commit check end, commitedIndex = %v, len(log) = %v\n", rf.me, rf.commitedIndex, len(rf.log))
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

func (rf *Raft) findInLogs(index int) (bool, int) {
	if rf.getLastLogIndex() < index {
		return false, -1
	} else {
		return true, index - rf.lastIncludedIndex - 1
	}
}

func Assert(f bool, msg string) {
	if !f {
		panic(msg)
	}
}

func (rf *Raft) handleSnapshot(peer_id int, leader_term int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}

	fChan := make(chan bool, 1)
	timerChan := time.After(time.Duration(rpcErrorInterval) * time.Millisecond)
	go func() {
		f := rf.sendInstallSnapshot(peer_id, args, &reply)
		fChan <- f
	}()

	f := false
	select {
	case <-timerChan:
		debuger.DPrintf("pid = %v, append snapshot to %v, timeout\n", rf.me, peer_id)
		return
	case f = <-fChan:
		break
	}
	close(fChan)

	debuger.DPrintf("pid = %v, append snapshot to %v, reply = %v, send ok? = %v\n", rf.me, peer_id, reply, f)
	if !f {
		return
	} else {
		rf.mu.Lock()
		// 检查是否还有权力
		if !rf.checkPower(leader_term) {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.status = Follower
			rf.voted_id = -1
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
			rf.commitCond.Signal()    // 降级也会唤醒
			rf.appendCond.Broadcast() // 通知所有关闭
		} else {
			// 发送成功
			// rf.nextIndex[peer_id] = rf.lastIncludedIndex + 1 // 发的可能是上一个snapshot，用现在的更新有问题的
			rf.nextIndex[peer_id] = args.LastIncludedIndex + 1
			rf.matchIndex[peer_id] = args.LastIncludedIndex
			rf.commitCond.Signal()
			rf.needCheck = true
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleEntrys(peer_id int, leader_term int, args *AppendEntryArgs) {
	reply := AppendEntryReply{}
	fChan := make(chan bool, 1)
	timerChan := time.After(time.Duration(rpcErrorInterval) * time.Millisecond)
	go func() {
		f := rf.sendAppendEntry(peer_id, args, &reply)
		fChan <- f
	}()

	f := false
	select {
	case <-timerChan:
		debuger.DPrintf("pid = %v, append entrys to %v, timeout\n", rf.me, peer_id)
		return
	case f = <-fChan:
		break
	}
	close(fChan)

	debuger.DPrintf("pid = %v, append entrys to %v, reply = %v, send ok? = %v\n", rf.me, peer_id, reply, f)
	if !f {
		return
	}

	rf.mu.Lock()
	if !rf.checkPower(leader_term) {
		rf.mu.Unlock()
		return
	}

	if !reply.Success {
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.status = Follower
			rf.voted_id = -1
			rf.alive_time = time.Now().UnixNano() / int64(time.Millisecond)
			rf.commitCond.Signal()    // 降级也会唤醒
			rf.appendCond.Broadcast() // 通知所有关闭
		} else {
			rf.nextIndex[peer_id] = reply.NextIndex
			debuger.DPrintf("pid = %v, append to %v not match, matchIndex = %v, nextIndex = %v, reply = %+v\n", rf.me, peer_id, rf.matchIndex[peer_id], rf.nextIndex[peer_id], reply)
		}
	} else {
		// 更新nextIndex和matchIndex，和发送的保持一致
		rf.matchIndex[peer_id] = rf.nextIndex[peer_id] + len(args.Entries) - 1
		rf.nextIndex[peer_id] = rf.matchIndex[peer_id] + 1
		rf.commitCond.Signal()
		rf.needCheck = true
		debuger.DPrintf("pid = %v, append to %v success, matchIndex = %v, nextIndex = %v\n", rf.me, peer_id, rf.matchIndex[peer_id], rf.nextIndex[peer_id])
	}
	rf.mu.Unlock()
}

func (rf *Raft) checkPower(leader_term int) bool {
	return rf.live && rf.status == Leader && rf.term == leader_term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	} else {
		return rf.log[len(rf.log)-1].Term
	}
}

func (rf *Raft) appendPeer(leader_term int, peer_id int) {
	for {
		rf.mu.Lock()

		latest_idx := rf.getLastLogIndex()
		debuger.DPrintf("pid = %v, start to append check to %v, latest_idx = %v, lastIncludeIndex = %v", rf.me, peer_id, latest_idx, rf.lastIncludedIndex)
		for rf.checkPower(leader_term) && rf.nextIndex[peer_id] > latest_idx {
			debuger.DPrintf("pid = %v, start to wait for peer_id = %v", rf.me, peer_id)
			rf.appendCond.Wait()
			latest_idx = rf.getLastLogIndex()
			debuger.DPrintf("pid = %v, weaked up to append check to %v, latest_idx = %v, nextIndex = %v", rf.me, peer_id, latest_idx, rf.nextIndex[peer_id])
		}

		if !rf.checkPower(leader_term) {
			rf.mu.Unlock()
			break
		}

		debuger.DPrintf("pid = %v start to append to %v, nextIndex = %v, len rf.log = %v, matchIndex = %v\n", rf.me, peer_id, rf.nextIndex[peer_id], len(rf.log), rf.matchIndex[peer_id])
		// 得到最新的index，等待过程中可能又提交了一些
		if rf.nextIndex[peer_id] <= latest_idx { // 还没同步到最新
			// 判断如果nextIndex在快照中，发快照
			if rf.nextIndex[peer_id] <= rf.lastIncludedIndex {
				// 发送快照并等待
				// 发送成功的话更新nextIndex和matchIndex
				args := InstallSnapshotArgs{
					Term:              leader_term,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(), // 发送当前的snapshot，这里读的很慢如果太大的话，可以考虑写复制
				}
				rf.mu.Unlock()
				debuger.DPrintf("pid = %v, send snapshot to %v, lastIncludeIndex = %v, rf.nextIndex = %v, snapshot size = %v\n", rf.me, peer_id, rf.lastIncludedIndex, rf.nextIndex[peer_id], len(args.Data))
				debuger.DPrintf("pid = %v, send snapshot to %v, start.\n", rf.me, peer_id)
				rf.handleSnapshot(peer_id, leader_term, &args)
				debuger.DPrintf("pid = %v, send snapshot to %v, return.\n", rf.me, peer_id)
			} else {
				f, idx := rf.findInLogs(rf.nextIndex[peer_id])
				Assert(f, "findInLogs error when append to peer")
				args := AppendEntryArgs{
					Term:         leader_term,
					LeaderId:     rf.me,
					Entries:      rf.log[idx:],
					LeaderCommit: rf.commitedIndex,
				}
				if idx == 0 {
					args.PrevLogIndex = rf.lastIncludedIndex
					args.PrevLogTerm = rf.lastIncludedTerm
				} else {
					args.PrevLogIndex = rf.log[idx-1].Index
					args.PrevLogTerm = rf.log[idx-1].Term
				}
				rf.mu.Unlock()
				debuger.DPrintf("pid = %v, send entrys to %v, start.\n", rf.me, peer_id)
				rf.handleEntrys(peer_id, leader_term, &args)
				debuger.DPrintf("pid = %v, send entrys to %v, return.\n", rf.me, peer_id)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() bool {
	// 群发选票，接收
	r := 150 + (rand.Int63() % 150)
	timer := time.After(time.Duration(r) * time.Millisecond)
	debuger.DPrintf("pid = %v, r = %v\n", rf.me, r)

	rf.mu.Lock()
	if rf.status != Candidate {
		rf.mu.Unlock()
		return false
	}

	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

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

	for !isTimeout { // isFollower的话直接转换
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
					debuger.DPrintf("pid = %v become leader, has logs = %v\n", rf.me, rf.log)
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

	return rf.checkStatus(Leader)
}

func (rf *Raft) sendHeartBeat(leader_term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.checkPower(leader_term) {
			rf.mu.Unlock()
			break
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				if !rf.checkPower(leader_term) {
					rf.mu.Unlock()
					return
				}
				args := AppendEntryArgs{}
				args.Term = rf.term
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitedIndex
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
						rf.commitCond.Signal()
						rf.appendCond.Broadcast()
					}
				}
				rf.mu.Unlock()
			}(i)
		}
		rf.mu.Unlock()

		debuger.DPrintf("pid = %v, send heartbeat, time = %v\n", rf.me, time.Now().UnixNano()/int64(time.Millisecond))

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
		peers:             peers,
		persister:         persister,
		me:                me,
		term:              0,
		status:            Follower,
		alive_time:        time.Now().UnixNano() / int64(time.Millisecond),
		voted_id:          -1,
		commitedIndex:     0,
		lastApplied:       0,
		applyCh:           applyCh,
		lastIncludedIndex: 0,
		live:              true,
	}
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendCond = sync.NewCond(&rf.mu)
	debuger.DPrintf("pid = %v remake raft = %v\n", rf.me, rf)

	// Your initialization code here (2A, 2B, 2C).
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	debuger.DPrintf("pid = %v, init, lastIncludedIndex = %v, lastIncludedTerm = %v, log_len = %v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))

	// 检查提交
	go rf.checkApply()  // 检查是否可以应用
	go rf.submitApply() // 应用队列中的

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
