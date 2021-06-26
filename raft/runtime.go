package raft

import (
	"time"
	"wujiah251/Raft/rpc"
)

// 运行相关函数

// 如果server不是leader，返回false
// 如果是leader，生成一个新的log entry，返回true
// 将命令附加到复制日志中
func (r *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	r.mutex.Lock()
	defer r.mutex.Unlock()

	// 获得最新的日志信息
	lastLogIndex, _ := r.getLastLogIndexTerm()
	index = lastLogIndex + 1
	term = r.currentTerm
	// 是否是领导者
	isLeader = r.role == LEADER
	if isLeader {
		// 创建新的日志
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		r.log = append(r.log, logEntry)
		DEBUG("[DEBUG] Server[%v]:(%s, Term:%v) get command %+v", r.id, r.GetRole(), r.currentTerm, command)
		r.matchIndex[r.id] = index
		r.Persist()
	}

	return index, term, isLeader
}

func (r *Raft) Kill() {

}

func Make(peers []*rpc.ClientEnd, id int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// TODO:
	DEBUG("[DEBUG] Server[%v]:Start Func Make()\n", id)
	defer DEBUG("[DEBUG] Server[%v]: End Func Make()\n", id)
	// 初始化Raft Server
	r := &Raft{}
	r.peers = peers
	r.persister = persister
	r.id = id

	r.currentTerm = 0
	r.votedFor = -1
	r.applyChan = applyCh
	r.lastApplied = 0
	r.committedIndex = 0

	// 初始化log，并添加一个空的守护日志（因为log的index从1开始）
	guideEntry := LogEntry{
		Command: nil,
		Term:    0,
		Index:   0,
	}
	r.log = append(r.log, guideEntry)
	r.role = FOLLOWER
	r.leaderId = -1
	r.ReadPersist(persister.ReadRaftState())

	// 初始化选举的计时器
	r.electionTimer = time.NewTimer(100 * time.Millisecond)
	r.heartBeatTimer = time.NewTimer(r.GetHeartBeatInterval())

	go r.ElectionLoop()
	go r.HeartBeatLoop()
	go r.ApplyLoop()

	return r
}
