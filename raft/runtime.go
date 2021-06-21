package raft

import "wujiah251/Raft/rpc"

// 运行相关函数

// 如果server不是leader，返回false
// 如果是leader，生成一个新的log entry，返回true
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
}
