package raft

import "time"

// 领导者选举loop，等待选举定时器超时，然后判断自己是否是定leader
// 如果不是leader，则发起一次选举：包括成为候选人和请求其他raft节点给自己投票
func (rf *Raft) LeaderElectionLoop() {
	for {
		// 等待 election timeout
		<-rf.electionTimer.C // 表达式会被Block直到超时
		rf.resetElectionTimer()

		rf.mu.Lock()
		DPrintf("[DEBUG] Server[%v]:(%s) Start Leader Election Loop", rf.me, rf.getRole())
		if rf.role == LEADER {
			DPrintf("[DEBUG] Server[%v]:(%s) End Leader Election Loop, Leader is Server[%v]", rf.me, rf.getRole(), rf.me)
			rf.mu.Unlock()
			continue
		}

		if rf.role == FOLLOWER || rf.role == CANDIDATE {
			rf.switchToCandidate()
			rf.mu.Unlock()
			rf.sendRequestVoteRPCToOthers()
		}
	}
}

func (rf *Raft) heartBeatLoop() {
	for {
		<-rf.heartBeatTimer.C
		rf.resetHeartBeatTimer()

		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		for slave := range rf.peers {
			// 不发送给自己
			if slave == rf.me {
				rf.nextIndex[slave] = len(rf.log) + 1
				rf.matchIndex[slave] = len(rf.log)
				continue
			} else {
				go rf.sendAppendEntriesRPCToPeer(slave)
			}
		}
	}
}

func (rf *Raft) apply(index int) {
	msg := ApplyMsg{
		Index:       index,
		Command:     rf.log[index].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}
	DPrintf("[DEBUG] Srv[%v](%s) apply log entry %+v", rf.me, rf.getRole(), rf.log[index].Command)
	rf.applyCh <- msg
}

func (rf *Raft) applyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			rf.apply(rf.lastApplied)
		}
		rf.mu.Unlock()
	}
}
