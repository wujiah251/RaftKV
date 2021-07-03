package raft

func (r *Raft) Lock(where string) {
	DPrintf("%s lock", where)
	r.mu.Lock()
}

func (r *Raft) Unlock(where string) {
	DPrintf("%s lock", where)
	r.mu.Unlock()
}
