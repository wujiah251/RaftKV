package raft

import (
	"sync"
	"time"
)

type ServerType int

const (
	LEADER    ServerType = 1 //领导者
	FOLLOWER  ServerType = 2 //追随者
	CANDIDATE ServerType = 3 //候选者
)

type ApplyMsg struct {
	index       int
	command     interface{}
	useSnapshot bool
	snapshot    []byte
}

type LogEntry struct {
	Command interface{} // 状态机的命令
	Term    int         // log entry的term
	Index   int         // log entry的index
}

type Raft struct {
	mutex sync.Mutex // 互斥锁

	// 持久化时期
	currentTerm int // server最新term，单调递增
	votedFor    int // 当前term中所投票的id，如果没有投票，则为null

	// 节点自身状态信息
	role           ServerType  // 节点状态
	leaderId       int         // 领导
	electionTimer  *time.Timer // Leader选举的定时器
	heartBeatTimer *time.Timer // 心跳的定时器

	applyChan chan ApplyMsg
}
