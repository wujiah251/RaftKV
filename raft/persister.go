package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type Persister struct {
	mutex     sync.Mutex
	raftState []byte
	snapshot  []byte
}

// 获得一个实例
func NewPersister() *Persister {
	return &Persister{}
}

// 拷贝函数
func (p *Persister) Copy() *Persister {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	res := NewPersister()
	res.raftState = p.raftState
	res.snapshot = p.snapshot
	return res
}

// 保存Raft状态
func (p *Persister) SaveRaftState(state []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.raftState = state
}

// 读Raft状态
func (p *Persister) ReadRaftState() []byte {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.raftState
}

// 获得Raft状态长度
func (p *Persister) RaftStateSize() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.raftState)
}

// 保存快照信息
func (p *Persister) SaveSnapshot(snapshot []byte) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.snapshot = snapshot
}

// 读取快照数据
func (p *Persister) ReadSnapshot() []byte {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.snapshot
}

func (r *Raft) Persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	_ = e.Encode(r.currentTerm)
	_ = e.Encode(r.votedFor)
	_ = e.Encode(r.log)
	data := w.Bytes()
	r.persister.SaveRaftState(data)
}

func (r *Raft) ReadPersist(data []byte) {
	b := bytes.NewBuffer(data)
	d := gob.NewDecoder(b)
	_ = d.Decode(&r.currentTerm)
	_ = d.Decode(&r.votedFor)
	_ = d.Decode(&r.log)
}
