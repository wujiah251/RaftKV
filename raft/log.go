package raft

import (
	"sort"
	"time"
)

// AppendLog失败后是否需要回退
// 最后，额外定义一个特殊值Backoff用于日志的追加发生异常时的处理
const (
	BackOff = -100
)

// 向其他sercer请求添加Log Entry的Request结构
type AppendEntriesReq struct {
	Term         int        // 领导者的term
	LeaderId     int        // 领导者的ID，
	PrevLogIndex int        // 在append新log entry前的log index
	PrevLogTerm  int        // 在append新log entry前的log index下的term
	Entries      []LogEntry // 要append log entries
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriesReply struct {
	// TODO:用户数据
	Term      int  // 时期
	Success   bool // 如果追随者包含有匹配preLogIndex和prevLogTerm的entry
	NextIndex int  // 下一个要append的Index，根据AppendEntries的情况来判断
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

func (r *Raft) AppendEntries(req AppendEntriesReq, reply *AppendEntriesReply) {
	// TODO:
	r.mutex.Lock()
	defer r.Persist()
	defer r.mutex.Unlock()

	// 初始化
	reply.Success = false
	reply.Term = r.currentTerm

	// 拒绝Term小于自己的节点的Append请求
	if r.currentTerm > req.Term {
		DEBUG("[DEBUG] Server[%v]:(%s) Reject AppendEntries due to currentTerm > req.Term", r.id, r.GetRole())
		return
	}
	// 判断心跳是否来自leader
	if len(req.Entries) == 0 {
		DEBUG("[DEBUG] Server[%v]:(%s, Term:%v) Get Heart Beats from %v", r.id, r.GetRole(), r.currentTerm, req.LeaderId)
	}

	r.currentTerm = req.Term
	r.becomeToFollower(req.Term)
	r.ResetElectionTimer() // 收到了有效的Leader的消息，重置选举的定时器

	// r.log[req.PrevLogIndex]有没有内容，即上一个应该同步的位置
	lastLogIndex, _ := r.getLastLogIndexTerm()
	if req.PrevLogIndex > lastLogIndex {
		DEBUG("[DEBUG] Server[%v]:(%s) Reject AppendEntries due to lastLogIndex < req.PrevLogIndex", r.id, r.GetRole())
		reply.NextIndex = r.GetNextIndex()
		return
	} else if r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		DEBUG("[DEBUG] Server[%v]:(%s) Previous log entries do not match", r.id, r.GetRole())
		reply.NextIndex = BackOff
	} else {
		reply.Success = true
		r.log = append(r.log[0:req.PrevLogIndex+1], req.Entries...)
	}

	if reply.Success {
		r.leaderId = req.LeaderId
		if req.LeaderCommit > r.committedIndex {
			lastLogIndex, _ := r.getLastLogIndexTerm()
			r.committedIndex = min(req.LeaderCommit, lastLogIndex)
			DEBUG("[DEBUG] Server[%v]:(%s) Follower Update commitIndex, lastLogIndex is %v", r.id, r.GetRole(), lastLogIndex)
		}
	}
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
	reply := AppendEntriesReply{}
	// RPC
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

		DEBUG("[DEBUG] Server[%v] (%s) Get reply for AppendEntries from %v, reply.Term <= r.currentTerm, reply is %+v", r.id, r.GetRole(), slave, reply)
		if reply.Success {
			lenEntry := len(req.Entries)
			// 已经匹配的日志条目索引
			r.matchIndex[slave] = req.PrevLogIndex + lenEntry
			r.nextIndex[slave] = r.matchIndex[slave] + 1
			DEBUG("[DEBUG] Server[%v] (%s): matchIndex[%v] is %v", r.id, r.GetRole(), slave, r.matchIndex[slave])
			majorityIndex := getMajoritySameIndex(r.matchIndex)
			if r.log[majorityIndex].Term == r.currentTerm && majorityIndex > r.committedIndex {
				r.committedIndex = majorityIndex
				DEBUG("[DEBUG] Server[%v](%s):Update commitIndex to %v", r.id, r.GetRole(), r.committedIndex)
			}
		} else {
			// 失败
			DEBUG("[DEBUG] Server[%v] (%s): append to Server[%v]Success is False, reply is %+v", r.id, r.GetRole(), slave, &reply)
			if reply.NextIndex > 0 {
				r.nextIndex[slave] = reply.NextIndex
			} else if reply.NextIndex == BackOff {
				// 直接后退一个term
				prevIndex := req.PrevLogIndex
				if prevIndex > 0 && r.log[prevIndex].Term == req.PrevLogTerm {
					prevIndex--
				}
				r.nextIndex[slave] = prevIndex + 1
			}
		}
		r.mutex.Unlock()
	}
}

// 心跳循环
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

		// 发送日志给slave
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

// 应用一条日志到状态机，然后添加消息到管道里面
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
