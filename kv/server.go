package raftkv

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"
	"wujiah251/RaftKV/raft"
	"wujiah251/RaftKV/rpc"
)

// 用于同步操作
type Op struct {
	Type   int       // 操作类型
	Key    string    // 键
	Value  string    // 数值
	Client int64     // 客户端
	Id     int64     // id
	Flag   chan bool // 同步判断是否执行完操作获取结果
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxRaftState int

	persister  *raft.Persister
	data       map[string]string //KV store
	waitingOps map[int]*Op
	opId       map[int64]int64 //记录每个客户端的操作id，用于控制幂等性
}

// RPC调用：Get
func (kv *RaftKV) Get(req *GetArgs, reply *GetReply) {
	var op Op
	op.Type = GetType
	op.Key = req.Key
	op.Client = req.ClientId
	op.Id = req.Id

	reply.WrongLeader = kv.Operate(op)

	if reply.WrongLeader {
		reply.Err = ErrWrongLeader
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if value, ok := kv.data[req.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *RaftKV) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:    req.Key,
		Value:  req.Value,
		Client: req.ClientId,
		Id:     req.Id,
	}
	if req.Op == Put {
		op.Type = PutType
	} else if req.Op == Append {
		op.Type = AppendType
	} else {
		fmt.Printf("Wrong PutAppendArgs op")
	}
	reply.WrongLeader = kv.Operate(op)
	if reply.WrongLeader {
		reply.Err = ErrWrongLeader
	} else {
		reply.Err = OK
	}
}

// 执行一次操作
func (kv *RaftKV) Operate(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		// 如果不是leader直接返回true
		return true
	}
	result := make(chan bool, 1)
	kv.mu.Lock()
	op.Flag = result
	kv.waitingOps[index] = &op
	kv.mu.Unlock()

	var ok bool
	timer := time.NewTimer(time.Second * 3)
	// 3秒内能否收到恢复
	select {
	case ok = <-result:
	case <-timer.C:
		ok = false
	}
	kv.mu.Lock()
	kv.waitingOps[index] = nil
	kv.mu.Unlock()
	if !ok {
		return true
	}
	return false
}

// 干掉一个KVServer，其实只需要干掉Raft节点
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
}

// 处理一个请求
func (kv *RaftKV) Handle(msg *raft.ApplyMsg) {
	// 加锁
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var args Op
	args = msg.Command.(Op)
	if kv.opId[args.Client] >= args.Id {
		// 幂等控制
	} else {
		switch args.Type {
		case PutType:
			kv.data[args.Key] = args.Value
		case GetType:
		// 不做任何操作
		case AppendType:
			kv.data[args.Key] = kv.data[args.Key] + args.Value

		}
		// 更新来源请求id
		kv.opId[args.Client] = args.Id
	}
	op := kv.waitingOps[msg.Index]
	if op != nil {
		if op.Client == args.Client && op.Id == args.Id {
			// 客户端相同、操作id相同
			// 同步完成
			op.Flag <- true
		} else {
			op.Flag <- false
		}
	}
}

// 启动一个kv服务器
func StartKVServer(servers []*rpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *RaftKV {
	gob.Register(Op{})
	kv := new(RaftKV)
	kv.me = me
	kv.maxRaftState = maxRaftState
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.waitingOps = make(map[int]*Op)
	kv.opId = make(map[int64]int64)

	go func() {
		for msg := range kv.applyCh {
			// 处理接收到的消息
			kv.Handle(&msg)
		}
	}()

	return kv
}
