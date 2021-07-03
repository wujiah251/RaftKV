package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
	"wujiah251/RaftKV/rpc"
)

// Raft中Server的三种状态
const (
	LEADER    = iota //领导者
	FOLLOWER         //追随者
	CANDIDATE        //候选者
)

// 时钟相关的
const (
	ElectionTimeoutMin = 100 //  选举时间间隔100-500ms
	ElectionTimeoutMax = 500
	HeartBeatsInterval = 100 // 心跳间隔100ms
)

// AppendLog失败后是否需要回退
// 最后，额外定义一个特殊值Backoff用于日志的追加发生异常时的处理
const (
	BackOff = -100
)

/*==========================================
	Struct 结构体定义
==========================================*/

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// 日志信息Log entry
type LogEntry struct {
	Command interface{} // 状态机的命令
	Term    int         // log entry的term
	Index   int         // log entry的index
}

// Raft节点对象
type Raft struct {
	mu          sync.Mutex
	peers       []*rpc.ClientEnd
	persister   *Persister
	me          int        // index into peers[]
	currentTerm int        // server已知的最新term，初始化为0，单调递增
	votedFor    int        // 当前term中所投票的id，如果没有投票，则为null
	log         []LogEntry // 日志列表，First index is 1

	// 所有服务器的不稳定状态
	commitIndex int // committed的最大的log entry index，初始化为0，单调递增
	lastApplied int // 应用到状态机的最大的log entry index，初始化为0，单调递增
	// Leader中的不稳定状态
	nextIndex  []int // To send, 对每个server，下一个要发送的log entry的序号， 初始化为 leader last log index+1 TODO: 初始化
	matchIndex []int // To replicated，对每个server，已知的最高的已经复制成功的序号

	role           int         // 服务器状态
	leaderID       int         // Leader的id
	electionTimer  *time.Timer // 选举leader的定时器
	heartBeatTimer *time.Timer // Heart Beat的定时器
	applyCh        chan ApplyMsg
}

type AppendEntriesRequest struct {
	Term         int        // 领导者的term
	LeaderId     int        // 领导者的ID，
	PrevLogIndex int        // 在append新log entry前的log index
	PrevLogTerm  int        // 在append新log entry前的log index下的term
	Entries      []LogEntry // 要append log entries
	LeaderCommit int        // 领导者的commitIndex
}

type AppendEntriesResponse struct {
	// 添加日志的响应
	Term      int  // 返回节点的时期
	Success   bool // 成功，
	NextIndex int  // 下一个要append的Index，根据AppendEntries的情况来判断
}

// 候选人请求其他节点为自己投票的请求
type RequestVoteRequest struct {
	Term         int // 候选人的term
	CandidateId  int // 候选人的ID
	LastLogIndex int // 候选人日志中最后一条的序号
	LastLogTerm  int // 候选人日志中最后一条的term
}

// 候选人投票响应
type RequestVoteResponse struct {
	Term        int  // 当前的term，用于使候选人更新状态
	VoteGranted bool // 若为真，则表示候选人接受了投票
}

/*==========================================
	时钟相关函数定义
==========================================*/

// 选举超时时间
func randomElectionTimeout() time.Duration {
	// 选举超时时间，100~500ms
	x := rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin
	n := time.Duration(x)
	timeout := n * time.Millisecond
	return timeout
}

// 重置选举计时器
func (rf *Raft) resetElectionTimer() {
	duration := randomElectionTimeout()
	rf.electionTimer.Reset(duration)
	//DPrintf("[DEBUG] Server[%v]:(%s) Reset ElectionTimer with %v\n", rf.me, rf.getRole(), duration)
}

// 获取心跳的间隔（ms为单位u）
func getHeartBeatInterval() time.Duration {
	return HeartBeatsInterval * time.Millisecond
}

func (rf *Raft) resetHeartBeatTimer() {
	duration := getHeartBeatInterval()
	rf.heartBeatTimer.Reset(duration)
	DPrintf("[DEBUG] Server[%v]:(%s) Reset HeartBeatTimer with %v\n", rf.me, rf.getRole(), duration)
}

/*==========================================
	Leader Election 选举函数定义
==========================================*/

// 成为leader
func (rf *Raft) switchToLeader() {
	rf.role = LEADER
	rf.leaderID = rf.me
	rf.resetElectionTimer()
	rf.heartBeatTimer.Reset(0) // 马上启动心跳机制

	// matchIndex[] 跟踪 Leader 和每个 Follower 匹配到的日志条目
	// nextIndex[] 保存要发送每个 Follower 的下一个日志条目
	n := len(rf.peers)
	nlog := len(rf.log)

	rf.matchIndex = make([]int, n)
	rf.nextIndex = make([]int, n)

	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = nlog
	}
	rf.matchIndex[rf.me] = nlog - 1
	rf.persist()
	DPrintf("[DEBUG] Server[%v]:(%s) switch to leader and reset heartBeatTimer 0.", rf.me, rf.getRole())
}

// 成为候选者
func (rf *Raft) switchToCandidate() {
	// 成为候选者，时期+1
	rf.currentTerm++
	rf.role = CANDIDATE
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	// 持久化
	rf.persist()
	DPrintf("[DEBUG] Server[%v]:(%s) switch to candidate.", rf.me, rf.getRole())
}

// 成为追随者
func (rf *Raft) switchToFollower(term int) {
	var flag bool
	if rf.role != FOLLOWER {
		flag = true
	}
	rf.role = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	//rf.resetElectionTimer()
	if flag {
		DPrintf("[DEBUG] Server[%v]:(%s) switch to follower.", rf.me, rf.getRole())
	}
	rf.persist()
}

// 获得最新的日志的索引和时期
func (rf *Raft) getLastLogIndexTerm() (lastLogIndex int, lastLogTerm int) {
	last := len(rf.log) - 1
	lastLogIndex = rf.log[last].Index
	lastLogTerm = rf.log[last].Term
	assert(last, lastLogIndex, fmt.Sprintf("Server[%v](%s) %+v, The slice index should be equal to lastLogIndex", rf.me, rf.getRole(), rf.log))
	return lastLogIndex, lastLogTerm
}

/*==========================================
	Log Entry 日志相关函数定义
==========================================*/

func getMajoritySameIndex(matchIndex []int) int {
	n := len(matchIndex)
	tmp := make([]int, n)
	copy(tmp, matchIndex)
	// 找到中位index
	sort.Sort(sort.Reverse(sort.IntSlice(tmp)))
	return tmp[n/2]
}

// 获取下一次要append的起始index
func (rf *Raft) getNextIndex() int {
	// append log entry后必须再调用一次否则会返回错误的结果
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	nextIndex := lastLogIndex + 1
	return nextIndex
}

/*==========================================
	Persistent 持久化相关函数定义
==========================================*/

// 模拟对日志文件的写入、读取

// 持久化当前状态，包括时期、投票目标、日志
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// 读取日志
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

/*==========================================
	其他函数定义
==========================================*/

// 获取节点的状态：是否是时期和是否是leader
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.role == LEADER
	rf.mu.Unlock()
	return term, isLeader
}

// log转换成日志
func (rf *Raft) logToString() string {
	res := ""
	for index, log := range rf.log {
		if index <= rf.commitIndex {
			res += fmt.Sprintf("{C*%v T*%v i*%v}", log.Command, log.Term, log.Index)
		} else {
			res += fmt.Sprintf("{C:%v T:%v i:%v}", log.Command, log.Term, log.Index)
		}
	}
	return res
}

func (rf *Raft) toString() string {
	return fmt.Sprintf("LID: %v;Term:%d;log:%s;commitIndex:%v;",
		rf.leaderID, rf.currentTerm, rf.logToString(), rf.commitIndex)
}

func (rf *Raft) toStringWithoutLog() string {
	return fmt.Sprintf("LID: %v;Term:%d;commitIndex:%v;",
		rf.leaderID, rf.currentTerm, rf.commitIndex)
}

func (rf *Raft) getRole() string {
	var role string
	switch rf.role {
	case LEADER:
		role = "Lead"
	case FOLLOWER:
		role = "Foll"
	case CANDIDATE:
		role = "Cand"
	}
	//return role + " " + rf.toStringWithoutLog()
	return role + " " + rf.toString()
	//return role
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func assert(a interface{}, b interface{}, msg interface{}) {
	if a != b {
		panic(msg)
	}
}
