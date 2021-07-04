package raft

// 获得投票请求
func (rf *Raft) getRequestVoteRequest() RequestVoteRequest {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	args := RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return args
}

// 给制定server发送投票请求
func (rf *Raft) sendRequestVote(server int, args RequestVoteRequest, reply *RequestVoteResponse) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送投票请求给其他的Raft节点
func (rf *Raft) sendRequestVoteRPCToOthers() {
	DPrintf("[DEBUG] Server[%v]:(%s) Begin sendRequestVoteRPCToOthers", rf.me, rf.getRole())
	n := len(rf.peers)
	voteCh := make(chan bool, n) // 接收来自各个节点的reply

	// 同时给其他的raft节点发送请求
	for server := range rf.peers {
		// 不发送给自己
		if server == rf.me {
			continue
		} else {
			args := rf.getRequestVoteRequest()
			// 开启新go routine，分别发送RequestVote给对应的server
			go func(server int) {
				reply := RequestVoteResponse{}
				ok := rf.sendRequestVote(server, args, &reply)
				if ok {
					voteCh <- reply.VoteGranted
				} else {
					voteCh <- false // 如果没有收到回复，视为不投票
				}
			}(server)
		}
	}

	// 统计投票结果
	replyCounter := 1 // 收到的回复
	validCounter := 1 // 投自己的回复
	for {
		vote := <-voteCh
		rf.mu.Lock()
		if rf.role == FOLLOWER {
			rf.mu.Unlock()
			DPrintf("[DEBUG] Server[%v]:(%s) has been a follower", rf.me, rf.getRole())
			return
		}
		rf.mu.Unlock()
		replyCounter++
		if vote == true {
			validCounter++
		}
		if replyCounter == n || // 所有人都投票了
			validCounter > n/2 || // 已经得到了majority投票
			replyCounter-validCounter > n/2 { // 已经有majority的投票人不是投自己
			break
		}
	}
	// 超过半数投票，成为leader
	if validCounter > n/2 {
		// 得到了majority投票，成为Leader
		rf.mu.Lock()
		rf.switchToLeader()
		rf.mu.Unlock()
	} else {
		DPrintf("[DEBUG] Server[%v]:(%s) get %v vote, Fails", rf.me, rf.getRole(), validCounter)
	}
}

// 请求投票给自己的方法
func (rf *Raft) RequestVote(args RequestVoteRequest, reply *RequestVoteResponse) {
	rf.mu.Lock()
	DPrintf("[DEBUG] Server[%v]:(%s, Term:%v) Start Func RequestVote with args:%+v", rf.me, rf.getRole(), rf.currentTerm, args)
	defer rf.mu.Unlock()
	defer DPrintf("[DEBUG] Server[%v]:(%s) End Func RequestVote with args:%+v, reply:%+v", rf.me, rf.getRole(), args, reply)

	// 初始化
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	if rf.currentTerm > args.Term {
		// 对方任期号落后于自己
		return
	} else if rf.currentTerm < args.Term {
		// 对方任期号领先于自己
		rf.switchToFollower(args.Term)
	}

	// 是否拒绝投票
	if rf.rejectVote(args) {
		return
	}

	if rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.resetElectionTimer()
		return
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}

	DPrintf("[DEBUG] Server[%v]:(%s) Vote for %v", rf.me, rf.getRole(), args.CandidateId)
}

// 是否拒绝投票：
// 自己是leader，拒绝投票
// 如果已经投票并且没有投给对方，拒绝投票
// 若自己时期领先于对方，拒绝投票；若自己时期落后于对方，给对方投票
// 若自己时期和对象相同，且自己的最新日志index领先于对方，拒绝给对方投票
// 其他情况不拒绝投票
func (rf *Raft) rejectVote(args RequestVoteRequest) bool {
	if rf.role == LEADER {
		return true
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return true
	}
	// $5.4.1的限制
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	// 如果自己最后一条日志的任期比对方落后，则不拒绝
	// 如果自己最后一条日志的任期比对方先进，则拒绝
	if lastLogTerm != args.LastLogTerm {
		return lastLogTerm > args.LastLogTerm
	}
	// 最后一条日志任期相等的话，比较日志槽号
	// 如果自己的槽号更加先进，则拒绝，否则接受
	return lastLogIndex > args.LastLogIndex
}

// AppendEntries相关

// 获取要append到其他节点的Log列表
func (rf *Raft) getAppendLogs(slave int) (prevLogIndex int, prevLogTerm int, entries []LogEntry) {
	nextIndex := rf.nextIndex[slave]
	lastLogIndex, lastLogTerm := rf.getLastLogIndexTerm()
	// 如果nextIndex不合法，则返回
	if nextIndex <= 0 || nextIndex > lastLogIndex {
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}
	// 将要添加的日志添加到entries
	entries = append([]LogEntry{}, rf.log[nextIndex:]...)
	// 之前的日志索引
	prevLogIndex = nextIndex - 1
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	return
}

// 获取要向其他节点AppendEntries的请求
func (rf *Raft) getAppendEntriesRequest(slave int) AppendEntriesRequest {
	prevLogIndex, preLogTerm, entries := rf.getAppendLogs(slave)
	args := AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  preLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

// RPC handler，提供给客户端的AppendEntries接口
func (rf *Raft) AppendEntries(req AppendEntriesRequest, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	// 添加完日志之后要做持久化
	defer rf.persist()
	defer rf.mu.Unlock()

	// 初始化
	reply.Success = false
	reply.Term = rf.currentTerm

	// 拒绝Term小于自己的节点的Append请求
	if rf.currentTerm > req.Term {
		// 失败
		DPrintf("[DEBUG] Server[%v]:(%s) Reject AppendEntries due to currentTerm > req.Term", rf.me, rf.getRole())
		return
	}

	// 判断是否是来自leader的心跳
	if len(req.Entries) == 0 {
		DPrintf("[DEBUG] Server[%v]:(%s, Term:%v) Get Heart Beats from %v", rf.me, rf.getRole(), rf.currentTerm, req.LeaderId)
	} else {
		DPrintf("[DEBUG] Server[%v]:(%s, Term:%v) Start Func AppendEntries with req:%+v", rf.me, rf.getRole(), rf.currentTerm, req)
		defer DPrintf("[DEBUG] Server[%v]:(%s) End Func AppendEntries with req:%+v, reply:%+v", rf.me, rf.getRole(), req, reply)
	}

	// 更新自己的当前时期为请求的时期
	rf.currentTerm = req.Term
	// 成为追随者（发起RPC方是一个leader，无论自己是什么，都调用一次switchToFollower）
	rf.switchToFollower(req.Term)
	// 重制选举计时器
	rf.resetElectionTimer() // 收到了有效的Leader的消息，重置选举的定时器

	// 考虑rf.log[req.PrevLogIndex]有没有内容，即上一个应该同步的位置
	lastLogIndex, _ := rf.getLastLogIndexTerm()
	if req.PrevLogIndex > lastLogIndex {
		// 如果请求认为的prevLogIndex领先与节点自己的最新logIndex
		DPrintf("[DEBUG] Server[%v]:(%s) Reject AppendEntries due to lastLogIndex < req.PrevLogIndex", rf.me, rf.getRole())
		// 更新对方要同步给的自己的下一个NextIndex
		reply.NextIndex = rf.getNextIndex()
	} else if rf.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		// 时期不匹配，这是一次错误的append，提醒leader要回退
		DPrintf("[DEBUG] Server[%v]:(%s) Previous log entries do not match", rf.me, rf.getRole())
		// 失败
		reply.NextIndex = BackOff
	} else {
		reply.Success = true
		// 拼接已有的日志和要添加的日志
		rf.log = append(rf.log[0:req.PrevLogIndex+1], req.Entries...) // [a:b]，左取右不取，如果有冲突就直接截断
	}
	// 如果调用成功
	if reply.Success {
		rf.leaderID = req.LeaderId
		if req.LeaderCommit > rf.commitIndex {
			// leader的提交领先于自己
			lastLogIndex, _ := rf.getLastLogIndexTerm()
			// 将自己的提交更新到领导者提交index和自己最新index的最小值
			rf.commitIndex = min(req.LeaderCommit, lastLogIndex)
			DPrintf("[DEBUG] Server[%v]:(%s) Follower Update commitIndex, lastLogIndex is %v", rf.me, rf.getRole(), lastLogIndex)
		}
	}
}

// leader调用，发送给其他服务器附加日志
func (rf *Raft) sendAppendEntries(server int, req AppendEntriesRequest, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

// 发送slave节点附加日志
func (rf *Raft) sendAppendEntriesRPCToPeer(slave int) {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	req := rf.getAppendEntriesRequest(slave)
	if len(req.Entries) > 0 {
		DPrintf("[DEBUG] Server[%v]:(%s) sendAppendEntriesRPCToPeer send to Server[%v]", rf.me, rf.getRole(), slave)
	}
	rf.mu.Unlock()
	reply := AppendEntriesResponse{}
	if ok := rf.sendAppendEntries(slave, req, &reply); !ok {
		// RPC调用失败
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		// RPC的时期领先于自己，可能原因（宕机后重新恢复）
		DPrintf("[DEBUG] Server[%v] (%s) Get reply for AppendEntries from %v, reply.Term > rf.currentTerm", rf.me, rf.getRole(), slave)
		// 这个应该成为追随者并重置选举定时器
		rf.switchToFollower(reply.Term)
		rf.resetElectionTimer()
		rf.mu.Unlock()
		return
	}

	// 自己不是Leader了，并且时期和请求时期不一致，不作处理
	if rf.role != LEADER || rf.currentTerm != req.Term {
		rf.mu.Unlock()
		return
	}

	DPrintf("[DEBUG] Server[%v] (%s) Get reply for AppendEntries from %v, reply.Term <= rf.currentTerm, reply is %+v", rf.me, rf.getRole(), slave, reply)
	if reply.Success {
		// 附加日志成功了
		lenEntry := len(req.Entries)
		// 更新对slave的匹配信息
		rf.matchIndex[slave] = req.PrevLogIndex + lenEntry
		rf.nextIndex[slave] = rf.matchIndex[slave] + 1
		DPrintf("[DEBUG] Server[%v] (%s): matchIndex[%v] is %v", rf.me, rf.getRole(), slave, rf.matchIndex[slave])
		// 获得到‍各个匹配index
		majorityIndex := getMajoritySameIndex(rf.matchIndex)
		// TODO：论文这个地方没看懂，为什么取中位数（向下取整）作为commitIndex（保证过半的服务器领先于commitIndex）
		if rf.log[majorityIndex].Term == rf.currentTerm && majorityIndex > rf.commitIndex {
			rf.commitIndex = majorityIndex
			DPrintf("[DEBUG] Server[%v] (%s): Update commitIndex to %v", rf.me, rf.getRole(), rf.commitIndex)
		}
	} else {
		// 附加日志失败，要重试（如果是BackOff，则后退）
		DPrintf("[DEBUG] Server[%v] (%s): append to Server[%v]Success is False, reply is %+v", rf.me, rf.getRole(), slave, &reply)
		if reply.NextIndex > 0 {
			rf.nextIndex[slave] = reply.NextIndex
		} else if reply.NextIndex == BackOff {
			// 直接后退一个term
			prevIndex := req.PrevLogIndex
			for prevIndex > 0 && rf.log[prevIndex].Term == req.PrevLogTerm {
				prevIndex--
			}
			rf.nextIndex[slave] = prevIndex + 1
		}
	}
	rf.mu.Unlock()
}
