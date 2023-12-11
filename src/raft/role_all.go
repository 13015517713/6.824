package raft

import (
	"time"

	debuger "6.5840/helper"
)

func (rf *Raft) meetBiggerTerm(term int) {
	Assert(term > rf.term, "not bigger term")
	rf.term = term
	rf.votedId = -1
	rf.status = Follower
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
		rf.applyList = rf.applyList[1:] // 模拟队列实现了，引用不是拷贝，复杂度是可以接收的o(1)
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
			return
		}

		rf.lastApplied++
		debuger.DPrintf("pid = %v, ready to apply index = %v, loglen = %v", rf.me, rf.lastApplied, len(rf.log))
		f, idx := rf.findInLogs(rf.lastApplied)
		Assert(f, "checkApplyOnce search index error")
		Assert(rf.log[idx].Index == rf.lastApplied, "checkApplyOnce search index error")

		applyMsgList := []ApplyMsg{}
		for {
			applyMsgList = append(applyMsgList, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[idx].Command,
				CommandIndex: rf.lastApplied,
			})

			if rf.lastApplied < rf.commitedIndex {
				rf.lastApplied++ // 可以把commit的全都放到发送队列中
				idx++
			} else {
				break
			}

			debuger.DPrintf("pid = %v, ready to apply success index = %v, loglen = %v", rf.me, rf.lastApplied, len(rf.log))
		}

		rf.applyLock.Lock()
		rf.applyList = append(rf.applyList, applyMsgList...)
		rf.applyLock.Unlock()

		debuger.DPrintf("pid = %v, applyMsg = %v, appliedlen = %v, hadcommitted = %v\n", rf.me, applyMsgList, rf.lastApplied, rf.commitedIndex)
		rf.mu.Unlock() // 一直占用锁，可能导致重新选举; sleep一段时间再触发

		time.Sleep(time.Duration(applyInterval) * time.Microsecond)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	debuger.DPrintf("pid = %v start Snapshot, index = %v, snapshot size = %v, lastApplied = %v\n", rf.me, index, len(snapshot), rf.lastApplied)
	if index <= rf.lastIncludedIndex {
		return
	}

	f, idx := rf.findInLogs(index)
	Assert(f, "snapshot error, not find index in logs")
	Assert(rf.log[idx].Index == index, "snapshot error, not find index in logs")

	rf.lastIncludedIndex, rf.lastIncludedTerm = index, rf.log[idx].Term
	rf.log = rf.log[idx+1:]

	debuger.DPrintf("pid = %v call Snapshot, index = %v, snapshot size = %v, lastApplied = %v\n", rf.me, index, len(snapshot), rf.lastApplied)
	rf.persistSnap(snapshot)
}
