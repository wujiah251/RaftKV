package raftkv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"wujiah251/RaftKV/rpc"
)

type Clerk struct {
	servers     []*rpc.ClientEnd
	leaderId    int
	clientId    int64
	currentOpId int64
}

func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.currentOpId = 0
	ck.clientId = nRand()
	return ck
}

func nRand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	x := bigX.Int64()
	return x
}

// 下面是提供给客户端的三个功能
// Get
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		Id:       atomic.AddInt64(&ck.currentOpId, 1),
	}
	for {
		var reply GetReply
		ck.servers[ck.leaderId].Call("RaftKV.Get", &req, &reply)

		if reply.WrongLeader == false && reply.Err == OK {
			return reply.Value
		} else {
			// TODO: 可以考虑返回参数定向leader
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

// Put和Append共享的方法
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// 每次请求id原子地增加1，保证幂等
	req := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		Id:       atomic.AddInt64(&ck.currentOpId, 1),
	}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", &req, &reply)
		if ok && reply.WrongLeader == false && reply.Err == OK {
			break
		} else if ok {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			// TODO:考虑让返回结果多出leaderId,-1表示自己也不是leaderId
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
