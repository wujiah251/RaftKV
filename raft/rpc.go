package raft

func (r *Raft) SendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 发送附加日志给server，返回RPC调用成功与否
func (r *Raft) SendAppendEntries(server int, req AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}
