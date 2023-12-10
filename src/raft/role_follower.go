package raft

import (
	"time"

	debuger "6.5840/helper"
)

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

// 当leader发现follower的nextindex在快照中，就会发快照，follower catchup快照内容
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 当nextindex已经在快照中就发，以保持同步
	// 也看作和appendEntry类似的方式，发完之后更新matchIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debuger.DPrintf("pid = %v, receive install snapshot, args = %+v\n", rf.me, args)

	if args.Term < rf.term {
		reply.Term = rf.term
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
		return
	}

	f, idx := rf.findInLogs(args.LastIncludedIndex)
	if !f {
		// 全用快照
		rf.log = []LogEntry{}
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
}

type AppendEntryArgs struct {
	Term     int
	LeaderId int
	// 用于匹配
	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	// 用于client同步commit
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
}

func (rf *Raft) FollowerTick() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.live {

		t := time.Now().UnixNano() / int64(time.Millisecond)
		a := rf.alive_time
		debuger.DPrintf("pid = %v, t = %v, a = %v, r = %v, interval = %v\n", rf.me, t, a, electInterval, t-a)

		if t-a < electInterval {
			// be alive
			rf.mu.Unlock()
			time.Sleep(time.Duration((electInterval - (t - a))) * time.Millisecond)
			rf.mu.Lock()
		} else {
			rf.term++
			rf.status = Candidate
			rf.voted_id = rf.me
			debuger.DPrintf("pid = %v become candidate\n", rf.me)
			return
		}
	}
}
