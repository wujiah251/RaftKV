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

// 获取要发送给slave的日志
func (r *Raft) GetAppendLogs(slave int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	// 要发送给slave的下一个日志index
	nextIndex := r.nextIndex[slave]
	lastLogIndex, lastLogTerm := r.getLastLogIndexTerm()

	if nextIndex <= 0 || nextIndex > lastLogIndex {
		// 发送给slave的下一个日志index小于0
		// 或者自己的最近的日志index落后与nextIndex
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	// 把nextIndex及之后的所有日志都发送给slave
	entries = append([]LogEntry{}, r.log[nextIndex:]...)
	prevLogIndex = nextIndex - 1
	// TODO:没看懂
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = r.log[prevLogIndex].Term
	}
	return
}

// 获取应该发送给slave的附加日志请求
func (r *Raft) GetAppendEntriesReq(slave int) AppendEntriesReq {
	// TODO:
	prevLogIndex, preLogTerm, entries := r.GetAppendLogs(slave)
	req := AppendEntriesReq{
		Term:         r.currentTerm,
		LeaderId:     r.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,          //要附加的日志条目
		LeaderCommit: r.committedIndex, //Leader提交信息
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

// 发送附加日志给server，返回RPC调用成功与否
func (r *Raft) SendAppendEntries(server int, req AppendEntriesReq, reply *AppendEntriseReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

// 发送添加日志给追随者slave
func (r *Raft) SendAppendEntriesRPCToPeer(slave int) {
	// TODO:
	r.mutex.Lock()
	// 不是leader则返回
	if r.role != LEADER {
		r.mutex.Unlock()
		return
	}
	req := r.GetAppendEntriesReq(slave)
	if len(req.Entries) > 0 {
		DEBUG("[DEBUG] Server[%v]:(%s) sendAppendEntriesRPCToPeer send to Server[%v]", r.id, r.GetRole(), slave)
	}
	r.mutex.Unlock()
	reply := AppendEntriseReply{}
	ok := r.SendAppendEntries(slave, req, &reply)
	if ok {
		// RPC调用成功
		r.mutex.Lock()
		if reply.Term > r.currentTerm {
			// 如果服务响应的日志大于自己，则自己成为follower
			DEBUG("[DEBUG] Server[%v] (%s) Get reply for AppendEntries from %v, reply.Term > r.currentTerm", r.id, r.GetRole(), slave)
			r.becomeToFollower(reply.Term)
			r.ResetElectionTimer()
			r.mutex.Unlock()
			return
		}

		if r.role != LEADER || r.currentTerm != req.Term {
			r.mutex.Unlock()
			return
		}
	}
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
