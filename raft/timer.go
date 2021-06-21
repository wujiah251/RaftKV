package raft

import (
	"log"
	"math/rand"
	"time"
)

/*==========================================
	时钟相关函数定义
==========================================*/


// 时钟相关的
// 选举间隔时间为100-500ms
// 心跳间隔时间为100ms
const (
	ElectionTimeoutMin = 100
	ElectionTimeoutMax = 500
	HeartBeatsInterval = 100 // 心跳间隔100ms
)

// 随机生成一个选举超时时间,单位为ms
func (r *Raft)RandomElectionTimeout() time.Duration {
	num := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)
	timeout := time.Duration(num)*time.Millisecond
	return timeout
}

// 获取心跳间隔时间，单位为ms
func (r *Raft)GetHeartBeatInterval() time.Duration {
	return HeartBeatsInterval * time.Millisecond
}

// 重置Leader选举定时器
func (r *Raft)ResetElectionTimer(){
	duration := r.RandomElectionTimeout()
	r.electionTimer.Reset(duration)
	log.Fatalf("[DEBUG] Reset ElectionTimer:%v",duration)
}

// 重置心跳定时器
func (r *Raft) ResetHeartBeatTimer() {
	duration := r.GetHeartBeatInterval()
	r.heartBeatTimer.Reset(duration)
	log.Fatalf("[DEBUG] Reset HeartBeatTimer:%v",duration)
}