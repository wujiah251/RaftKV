package raft

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"wujiah251/Raft/rpc"
)

// 配置文件

type Config struct {
	mutex     sync.Mutex    //  互斥锁
	t         *testing.T    // 测试
	rafts     []*Raft       // 节点列表
	net       *rpc.Network  // 网络
	saved     []*Persister  // 持久化
	n         int           // TODO:不知道
	done      int32         // TODO:
	applyErr  []string      // 来自apply Channel readers
	connected []bool        // 每个服务器是否在网络中
	endNames  [][]string    // port file names each sends to
	logs      []map[int]int // 每个服务器的已提交日志的拷贝
}

// 创建一个配置中心
func MakeConfig(t *testing.T, n int, unreliable bool) *Config {
	// 设置逻辑CPU数量
	runtime.GOMAXPROCS(4)
	cfg := &Config{}
	cfg.t = t
	cfg.net = rpc.MakeNetwork()
	cfg.applyErr = make([]string, cfg.n)
	cfg.rafts = make([]*Raft, cfg.n)
	cfg.connected = make([]bool, cfg.n)
	cfg.saved = make([]*Persister, cfg.n)
	cfg.logs = make([]map[int]int, cfg.n)

	cfg.SetUnreliable(unreliable)

	cfg.net.LongDelays(true)

	// 启动所有服务器
	for i := 0; i < n; i++ {
		cfg.logs[i] = map[int]int{}
		cfg.Start(i)
	}

	// 连接所有服务器
	for i := 0; i < cfg.n; i++ {
		cfg.Connect(i)
	}

	return cfg
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
	for j := 0; j < c.n; j++ {
		// 生成端口
		c.endNames[i][j] = RandString(20)
	}

	// 为server创建客户端
	ends := make([]*rpc.ClientEnd, c.n)
	for j := 0; j < c.n; j++ {
		// 创建一个客户端
		ends[j] = c.net.MakeEnd(c.endNames[i][j])
		c.net.Connect(c.endNames[i][j], j)
	}

	c.mutex.Lock()
	if c.saved[i] != nil {
		c.saved[i] = c.saved[i].Copy()
	} else {
		c.saved[i] = NewPersister()
	}
	c.mutex.Unlock()

	// 日志成功提交返回一个ApplyMsg
	applyCh := make(chan ApplyMsg)
	go func() {
		for m := range applyCh {
			errMsg := ""
			if m.UseSnapshot {
				// 忽视快照
			} else if v, ok := (m.Command).(int); ok {
				c.mutex.Lock()
				for j := 0; j < len(c.logs); j++ {
					if old, oldOk := c.logs[j][m.Index]; oldOk && old != v {
						// 一些服务器已经提交一个不同的数值
						errMsg = fmt.Sprintf("commit index=%v server=%v %v != server=%v %v",
							m.Index, i, m.Command, j, old)
					}
				}
				_, prevOk := c.logs[i][m.Index-1]
				c.logs[i][m.Index] = v
				c.mutex.Unlock()

				if m.Index > 1 && prevOk == false {
					errMsg = fmt.Sprintf("server %v apply out of order %v", i, m.Index)
				}
			} else {
				errMsg = fmt.Sprintf("committed command %v is not a int", m.Command)
			}

			if errMsg != "" {
				log.Fatalf("apply error:%v\n", errMsg)
				c.applyErr[i] = errMsg
			}
		}
	}()

	// 创建一个Raft实例
	r := Make(ends, i, c.saved[i], applyCh)
	c.mutex.Lock()
	c.rafts[i] = r
	c.mutex.Unlock()

	// 创建一个服务（R对于配置中心而言）
	service := rpc.MakeService(r)
	// 创建一个服务器
	server := rpc.MakeServer()

	server.AddService(service)
	c.net.AddServer(i, server)
}

func (c *Config) CleanUp() {
	for i := 0; i < len(c.rafts); i++ {
		if c.rafts[i] != nil {
			c.rafts[i].Kill()
		}
	}
	// TODO:
	atomic.StoreInt32(&c.done, 1)
}

// 连接一个节点
func (c *Config) Connect(i int) {
	c.connected[i] = true

	// TODO:
	for j := 0; j < c.n; j++ {
		if c.connected[j] {
			endName := c.endNames[i][j]
			c.net.Enable(endName, true)
		}
	}
	// TODO:
	for j := 0; j < c.n; j++ {
		if c.connected[j] {
			endName := c.endNames[j][i]
			c.net.Enable(endName, true)
		}
	}
}

// 和一个节点断开连接
func (c *Config) DisConnect(i int) {
	c.connected[i] = false

	// TODO:
	for j := 0; j < c.n; j++ {
		if c.endNames[i] != nil {
			endName := c.endNames[i][j]
			c.net.Enable(endName, false)
		}
	}

	// TODO:
	for j := 0; j < c.n; j++ {
		if c.endNames[i] != nil {
			endName := c.endNames[i][j]
			c.net.Enable(endName, false)
		}
	}
}

//
func (c *Config) RpcCount(server int) int {
	return c.net.GetCount(server)
}

// TODO:
func (c *Config) SetUnreliable(unrel bool) {
	c.net.Reliable(!unrel)
}

func (c *Config) SetLongReordering(longrel bool) {
	c.net.LongReordering(longrel)
}

// 检查是否存在一个leader
func (c *Config) CheckOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		// key:term
		// value: list of leaders
		leaders := make(map[int][]int)
		for i := 0; i < c.n; i++ {
			if c.connected[i] {
				if term, leader := c.rafts[i].GetState(); leader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		// 最新的时期
		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				// TODO:
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}
		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	// TODO:
	return -1
}

// 检查没有leader
func (c *Config) CheckNoLeader() {
	for i := 0; i < c.n; i++ {
		if c.connected[i] {
			_, isLeader := c.rafts[i].GetState()
			if isLeader {
				// TODO:
			}
		}
	}
}

// 获得认为一个log已经持久化了的服务器的个数
func (c *Config) CountCommitted(index int) (int, interface{}) {
	count := 0
	cmd := -1
	for i := 0; i < len(c.rafts); i++ {
		if c.applyErr[i] != "" {
			// TODO:
		}

		c.mutex.Lock()
		cmd1, ok := c.logs[i][index]
		c.mutex.Unlock()
		if ok {
			if count > 0 && cmd != cmd1 {
				// TODO:
			}
			count += 1
			cmd = cmd1
		}
	}
	return count, cmd
}

// 等待至少n个服务器commit
func (c *Config) Wait(index int, n int, startTerm int) interface{} {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		curNumber, _ := c.CountCommitted(index)
		if curNumber >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			// 每次等待时间翻倍
			to *= 2
		}
		if startTerm > -1 {
			for _, r := range c.rafts {
				if t, _ := r.GetState(); t > startTerm {
					return -1
				}
			}
		}
	}
	curNumber, cmd := c.CountCommitted(index)
	if curNumber < n {
		// TODO:
	}
	return cmd
}

// 执行一次命令
// cmd就是命令，expectedServers是期望提交的server数量
func (c *Config) One(cmd int, expectedServers int) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		index := -1
		for i := 0; i < c.n; i++ {
			starts = (starts + 1) % c.n
			var r *Raft
			c.mutex.Lock()
			if c.connected[starts] {
				r = c.rafts[starts]
			}
			c.mutex.Unlock()
			if r != nil {
				index1, _, ok := r.Start(cmd)
				if ok {
					index = index1
					break
				}
			}
		}

		if index != -1 {
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				count, cmd1 := c.CountCommitted(index)
				if count > 0 && count >= expectedServers {
					if cmd2, ok := cmd1.(int); ok && cmd2 == cmd {
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
	// TODO:
	return -1
}
