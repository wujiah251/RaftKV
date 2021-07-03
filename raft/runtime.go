package raft

import (
	"time"
	"wujiah251/RaftKV/rpc"
)

/*==========================================
	Raft 运行函数定义
==========================================*/

// 执行一个命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	index = lastLogIndex + 1
	term = rf.currentTerm
	isLeader = rf.role == LEADER

	if isLeader {
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.log = append(rf.log, logEntry)
		DPrintf("[Debug] Server[%v]:(%s, Term:%v) get command %+v", rf.me, rf.getRole(), rf.currentTerm, command)
		rf.matchIndex[rf.me] = index
		rf.persist()
	}

	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

// 创建一个Raft实例
func Make(peers []*rpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// TODO: Modify Make() to create a background goroutine that starts an election by sending out RequestVote RPC when it hasn't heard from another peer for a while
	DPrintf("[DEBUG] Server[%v]: Start Func Make()\n", me)
	defer DPrintf("[DEBUG] Server[%v]: End Func Make()\n", me)
	// 初始化 Raft Server状态
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.votedFor = -1 // 用-1表示null
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0

	// 初始化log，并加入一个空的守护日志（因为log的index从1开始）
	guideEntry := LogEntry{
		Command: nil,
		Term:    0,
		Index:   0,
	}
	rf.log = append(rf.log, guideEntry)
	rf.role = FOLLOWER
	rf.leaderID = -1
	rf.readPersist(persister.ReadRaftState())

	// 初始化选举的计时器
	rf.electionTimer = time.NewTimer(100 * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(getHeartBeatInterval())

	// Sever启动时，是follower状态。 若收到来自leader或者candidate的有效PRC，就持续保持follower状态。
	go rf.LeaderElectionLoop()
	go rf.heartBeatLoop()
	go rf.applyLoop()

	return rf
}
