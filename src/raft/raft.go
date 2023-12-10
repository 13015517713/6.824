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

	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	debuger "6.5840/helper"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// 常用配置
const (
	// ms
	heartBeatInterval  int64 = 100
	electInterval      int64 = 300
	rpcErrorInterval   int64 = 500 // 500ms不返回，rpc失败
	selectIdleInterval int64 = 10
	// us
	applyInterval int64 = 500
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term, rf.status == Leader
}

func (rf *Raft) checkPower(leader_term int) bool {
	return rf.live && rf.status == Leader && rf.term == leader_term
}

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

	// wait to do real apply
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
	live bool // guard by mu
}

// 标识RSM每一条日志
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
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

func (rf *Raft) findInLogs(index int) (bool, int) {
	if rf.getLastLogIndex() < index {
		return false, -1
	} else {
		return true, index - rf.lastIncludedIndex - 1
	}
}

// 需要持久化的raft变量
type RaftPersist struct {
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

// 写入snapshot时需要同时保存当前状态，故障回复
func (rf *Raft) persistSnap(snapshot []byte) {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	encoder.Encode(rf.getPersistField())
	data := buffer.Bytes()
	// debuger.DPrintf("pid = %v, persist, rf = %v, len = %v\n", rf.me, rf.getPersistField(), len(data))
	rf.persister.Save(data, snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// 读取持久化的状态
	var p RaftPersist
	decoder := labgob.NewDecoder(bytes.NewBuffer(data))
	decoder.Decode(&p)
	rf.setPersistField(&p)
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitedIndex = rf.lastIncludedIndex
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

	if rf.status != Leader {
		debuger.DPrintf("pid = %v, not leader\n", rf.me)
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1
	term := rf.term
	isLeader := true

	e := LogEntry{
		Command: command,
		Term:    rf.term,
		Index:   index,
	}

	rf.log = append(rf.log, e)
	rf.persist()
	rf.appendCond.Broadcast() // 通知所有channel去发logs

	debuger.DPrintf("pid = %v, receive client command, entry = %+v\n", rf.me, e)

	return index, term, isLeader
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
			rf.FollowerTick()

		case Candidate:
			rf.CandidateTick()

		case Leader:
			rf.LeaderTick()
		}
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
		nextIndex:         make([]int, len(peers)),
		matchIndex:        make([]int, len(peers)),
	}
	rf.commitCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendCond = sync.NewCond(&rf.mu)
	debuger.DPrintf("pid = %v remake raft = %v\n", rf.me, rf)

	// Your initialization code here (2A, 2B, 2C).
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	debuger.DPrintf("pid = %v, init, lastIncludedIndex = %v, lastIncludedTerm = %v, log_len = %v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, len(rf.log))

	// 检查提交
	go rf.checkApply()  // 检查是否可以应用，非阻塞的加入applyList（直接给applyChan会阻塞）
	go rf.submitApply() // 可赢

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
