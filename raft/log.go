package raft

import (
	"sort"
	"time"
)

// TODO:这个地方还没看懂
type AppendEntriesReq struct {
	Term         int        // 领导者的term
	LeaderId     int        // 领导者的ID，
	PrevLogIndex int        // 在append新log entry前的log index
	PrevLogTerm  int        // 在append新log entry前的log index下的term
	Entries      []LogEntry // 要append log entries
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriseReply struct {
	// TODO:用户数据
	Term int // 时期

}

// 日志相关函数定义

func getMajoritySameIndex(matchIndex []int) int {
	n := len(matchIndex)
	tmp := make([]int, n)
	copy(tmp, matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	return tmp[n/2]
}

func (r *Raft) GetAppendLogs(slave int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	return
}

func (r *Raft) GetAppendEntriesReq(slave int) AppendEntriesReq {
	// TODO:
	// prevLogIndex, preLogTerm, entries := r.GetAppendLogs(slave)
	req := AppendEntriesReq{
		Term: r.currentTerm,
	}
	return req
}

// 获取下一个索引
func (r *Raft) GetNextIndex() int {
	// 获取最新的日志索引
	lastLogIndex, _ := r.getLastLogIndexTerm()
	// 更新到新的索引
	nextIndex := lastLogIndex + 1
	return nextIndex
}

func (r *Raft) AppendEntries(req AppendEntriesReq, reply *AppendEntriesReq) {
	// TODO:
}

func (r *Raft) SendAppendEntries(server int, req AppendEntriesReq, reply *AppendEntriseReply) bool {
	// TODO:
	return false
}

func (r *Raft) SendAppendEntriesRPCToPeer(slave int) {
	// TODO:
}

func (r *Raft) HeartBeatLoop() {
	for {
		<-r.heartBeatTimer.C
		// 重置心跳
		r.ResetHeartBeatTimer()
		r.mutex.Lock()
		if r.role != LEADER {
			// 不是leader，不需要完成心跳工作
			r.mutex.Unlock()
			continue
		}
		r.mutex.Unlock()
		for slave := range r.peers {
			if slave == r.id {
				// 不用发给自己
				r.nextIndex[slave] = len(r.log) + 1
				r.matchIndex[slave] = len(r.log)
				continue
			} else {
				go r.SendAppendEntriesRPCToPeer(slave)
			}
		}
	}
}

func (r *Raft) Apply(index int) {
	msg := ApplyMsg{
		Index:       index,
		Command:     r.log[index].Command,
		UseSnapshot: false,
		Snapshot:    nil,
	}
	DEBUG("[DEBUG] Server[%v](%s) apply log entry %+v", r.id, r.GetRole(), r.log[index].Command)
	r.applyChan <- msg
}

func (r *Raft) ApplyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)
		r.mutex.Lock()
		for r.lastApplied < r.committedIndex {
			r.lastApplied++
			r.Apply(r.lastApplied)
		}
		r.mutex.Unlock()
	}
}
