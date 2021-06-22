package raft

import (
	"sync"
	"wujiah251/Raft/rpc"
)

// 配置文件

type Config struct {
	mutex    sync.Mutex   //  互斥锁
	rafts    []*Raft      // 节点列表
	net      *rpc.Network // 网络
	saved    []*Persister // 持久化
	n        int          // TODO:不知道
	endNames [][]string   // port file names each sends to
}

// 关闭一个节点，但是保存其持久化状态
func (c *Config) Shutdown(i int) {
	// 断开连接
	c.DisConnect(i)
	// 删除服务器，关闭客户端到服务器的连接
	c.net.DeleteServer(i)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 持久化,TODO:不懂
	if c.saved[i] != nil {
		c.saved[i] = c.saved[i].Copy()
	}

	r := c.rafts[i]
	if r != nil {
		// TODO:不知道为什么要解锁
		c.mutex.Unlock()
		r.Kill()
		c.mutex.Lock()
		c.rafts[i] = nil
	}

	if c.saved[i] != nil {
		raftLog := c.saved[i].ReadRaftState()
		c.saved[i] = &Persister{}
		c.saved[i].SaveRaftState(raftLog)
	}
}

// 启动一个server，或者说重启一个server
// 如果它已经存在，那就先kill
// 分配一个port给它
// 分配一个persister
func (c *Config) Start(i int) {
	c.Shutdown(i)
	c.endNames[i] = make([]string, c.n)
}

// 连接一个节点
func (c *Config) Connect(i int) {

}

// 和一个节点断开连接
func (c *Config) DisConnect(i int) {

}

//
