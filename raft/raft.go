package raft

import (
	"time"
)

type Raft struct{
	electionTimer *time.Timer 	// Leader选举的定时器
	heartBeatTimer *time.Timer // 心跳的定时器
}

