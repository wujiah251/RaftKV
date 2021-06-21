package raft

import (
	"fmt"
	"sync"
	"time"
	"wujiah251/Raft/rpc"
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
	mutex     sync.Mutex       // 互斥锁
	peers     []*rpc.ClientEnd // 其他节点数组
	persister *Persister       // 持久化对象
	id        int              // 自身id

	// 持久化时期
	currentTerm int        // server最新term，单调递增
	votedFor    int        // 当前term中所投票的id，如果没有投票，则为null
	log         []LogEntry // 日志

	// 所有服务器的不稳定状态
	committedIndex int // 已经提交的最大的log entry的index
	lastApplied    int // 应用到状态机的最大log entry的index

	// Leader中的不稳定状态
	nextIndex  []int // 对每个server，下一个要发送的log entry的序号
	matchIndex []int // 对每个server，已知的最高的已经复制成功的序号

	// 节点自身状态信息
	role           ServerType  // 节点状态
	leaderId       int         // 领导
	electionTimer  *time.Timer // Leader选举的定时器
	heartBeatTimer *time.Timer // 心跳的定时器

	applyChan chan ApplyMsg
}

// 请求投票参数
type RequestVoteArgs struct {
	Term         int // 候选人的term
	CandidateId  int //候选人的id
	LastLogIndex int // 候选人日志中最后一条的序号
	LastLogTerm  int // 候选人日志中最后一条term
}

// 请求投票的响应
type RequestVoteReply struct {
	// TODO:可以添加用户数据
	Term        int  // 当前的term，用于使候选人更新状态
	VoteGranted bool // 若为真，则表示候选人接受了投票
}

// 状态机

// 当选leader
func (r *Raft) becomeToLeader() {
	// 更新状态
	r.role = LEADER
	r.leaderId = r.id

	// 重置选举定时器
	r.ResetElectionTimer()
	// 启动心跳机制
	r.heartBeatTimer.Reset(0)
	// 节点个数
	n := len(r.peers)

	// 日志个数
	nlog := len(r.log)

	// 已经匹配的日志条目
	r.matchIndex = make([]int, n)
	// 保存要发送给每个Follower的下一个日志条目
	r.nextIndex = make([]int, n)

	for i := range r.peers {
		r.matchIndex[i] = 0
		r.nextIndex[i] = nlog
	}
	// 和自己匹配到了最新的日志条目：nlog-1
	r.matchIndex[r.id] = nlog - 1
	// 持久化
	r.Persist()
	DEBUG("[DEBUG] Server[%v]:(%s) switch to leader and reset heartBeatTimer 0.", r.id, r.GetRole())
}

// 成为追随者
func (r *Raft) becomeToFollower(term int) {
	// 当前是否没有追随他人
	var flag bool
	if r.role != FOLLOWER {
		flag = true
	}
	// 更新状态
	r.role = FOLLOWER
	r.currentTerm = term
	r.votedFor = -1

	if flag {
		DEBUG("[DEBUE] Server[%v]:(%s)switch to follower.", r.id, r.GetRole())
	}
	// 持久化
	r.Persist()
}

// 成为候选者
func (r *Raft) becomeToCandidate() {
	r.currentTerm++
	r.role = CANDIDATE
	// 投给自己
	r.votedFor = r.id
	// 重置选举定时器
	r.ResetElectionTimer()
	r.Persist()
	DEBUG("[DEBUG] Server[%v]:(%s) switch to candidate.", r.id, r.GetRole())
}

// 获取最新的日志索引和时期
func (r *Raft) getLastLogIndexTerm() (lastLogIndex int, lastLogTerm int) {
	last := len(r.log) - 1
	lastLogIndex = r.log[last].Index
	lastLogTerm = r.log[last].Term
	if last != lastLogIndex {
		msg := fmt.Sprintf("Server[%v](%s) %+v, The slice index should be equal to lastLogIndex", r.id, r.GetRole(), r.log)
		panic(msg)
	}
	return lastLogIndex, lastLogTerm
}

// 得到（生成）请求投票的参数
func (r *Raft) getRequestVoteArgs() RequestVoteArgs {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	lastLogIndex, lastLogTerm := r.getLastLogIndexTerm()
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

// 将请求投票发给其他节点
// 统计收到的响应中同意自己成为leader的票数
// 成为leader或者失败
func (r *Raft) sendRequestVoteToOthers() {
	DEBUG("[DEBUG] Server[%v]:(%s) Begin sendRequestVoteRPCToOthers", r.id, r.GetRole())
	n := len(r.peers)
	voteCh := make(chan bool, n) // 接收来自各个节点的reply

	for server := range r.peers {
		// 不给自己发
		if server == r.id {
			continue
		} else {
			args := r.getRequestVoteArgs()
			go func(server int) {
				reply := RequestVoteReply{}
				// 发送请求投票给自己（一次RPC）
				ok := r.SendRequestVote(server, args, &reply)
				if ok {
					// 对方投票
					voteCh <- reply.VoteGranted
				} else {
					voteCh <- false // 没有收到回复则视为不投票
				}
			}(server)
		}
	}

	// 统计投票结果
	replyCounter := 1 // 收到的回复
	votedCounter := 1 // 同意的回复
	for {
		vote := <-voteCh
		r.mutex.Lock()
		if r.role == FOLLOWER {
			// 如果自己成为了追随者
			DEBUG("[DEBUG] Server[%v]:(%s) has been a follower", r.id, r.GetRole())
			return
		}
		r.mutex.Unlock()
		replyCounter++
		if vote == true {
			votedCounter++
		}
		if replyCounter == n ||
			votedCounter > n/2 ||
			replyCounter-votedCounter > n/2 {
			break
		}
	}
	// 统计完成
	if votedCounter > n/2 {
		// 得票超过半数，成为leader
		r.mutex.Lock()
		r.becomeToLeader()
		r.mutex.Unlock()
	} else {
		DEBUG("[DEBUG] server[%v]:(%s) get %v vote, Fails", r.id, r.GetRole(), votedCounter)
	}
}

// 领导选举主循环
func (r *Raft) ElectionLoop() {
	for {
		// 等待超时
		<-r.electionTimer.C // 阻塞直到超时
		// 重置选举定时器
		r.ResetElectionTimer()

		r.mutex.Lock()
		DEBUG("[DEBUG] Server[%v]:(%s) Start Leader Election Loop", r.id, r.GetRole())
		if r.role == LEADER {
			DEBUG("[DEBUG] Server[%v]:(%s) End Leader Election Loop, Leader is Server[%v]", r.id, r.GetRole(), r.id)
			r.mutex.Unlock()
			continue
		}

		if r.role == FOLLOWER || r.role == CANDIDATE {
			// TODO:不用判断Leader存不存在吗
			// 成为候选者，发起一次投票
			r.becomeToCandidate()
			r.mutex.Unlock()
			r.sendRequestVoteToOthers()
		}
	}
}

// 请求投票
func (r *Raft) RequestVote(req RequestVoteArgs, reply *RequestVoteReply) {
	r.mutex.Lock()
	DEBUG("[DEBUG] Server[%v]:(%s, Term:%v) Start Func RequestVote with req:%+v", r.id, r.GetRole(), r.currentTerm, req)
	defer r.mutex.Unlock()
	defer DEBUG("[DEBUG] Server[%v]:(%s) End func RequestVote with req:%+v, reply:%+v", r.id, r.GetRole(), req, reply)

	// 初始化
	reply.VoteGranted = false
	reply.Term = r.currentTerm

	// 如果自己的时期大于请求的时期
	if r.currentTerm > req.Term {
		return
	} else if r.currentTerm < req.Term {
		// 落后于请求时期，则成为追随者
		r.becomeToFollower(req.Term)
	}

	// 时期相等才会进行到这里
	// TODO:
	if r.rejectVote(req) {
		return
	}

}

// 拒绝投票
func (r *Raft) rejectVote(req RequestVoteArgs) bool {
	// 如果自己是leader，就拒绝
	if r.role == LEADER {
		return true
	}
	// 如果自己已经给别人投票了，投票的人不是这次请求的来源
	if r.votedFor != -1 && r.votedFor != req.CandidateId {
		return true
	}

	// 获得最新的日志索引和日志时期
	lastLogIndex, lastLogTerm := r.getLastLogIndexTerm()
	// TODO:这个地方不太懂
	if lastLogTerm != req.LastLogTerm {
		return lastLogTerm > req.LastLogTerm
	}
	return lastLogIndex > req.LastLogIndex
}
