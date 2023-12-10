package raft

import (
	"time"

	debuger "6.5840/helper"
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

func (rf *Raft) LeaderTick() {
	rf.initLeader() // 心跳，检查commit，初始化NextIndex和MatchIndex

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
