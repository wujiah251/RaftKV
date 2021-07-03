package raftkv

import (
	crand "crypto/rand"
	"encoding/base64"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"wujiah251/RaftKV/raft"
	"wujiah251/RaftKV/rpc"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

// Randomize server handles
func random_handles(kvh []*rpc.ClientEnd) []*rpc.ClientEnd {
	sa := make([]*rpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	tag          string
	net          *rpc.Network
	n            int
	kvservers    []*RaftKV
	saved        []*raft.Persister
	endnames     [][]string // names of each server's sending ClientEnds
	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int
}

func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			cfg.kvservers[i].Kill()
		}
	}
}

// Maximum log size across all servers
func (cfg *config) LogSize() int {
	logsize := 0
	for i := 0; i < cfg.n; i++ {
		n := cfg.saved[i].RaftStateSize()
		if n > logsize {
			logsize = n
		}
	}
	return logsize
}

// attach server i to servers listed in to
// caller must hold cfg.mu
func (cfg *config) connectUnlocked(i int, to []int) {
	// log.Printf("connect peer %d to %v\n", i, to)

	// outgoing socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[i][to[j]]
		cfg.net.Enable(endname, true)
	}

	// incoming socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[to[j]][i]
		cfg.net.Enable(endname, true)
	}
}

func (cfg *config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}

// detach server i from the servers listed in from
// caller must hold cfg.mu
func (cfg *config) disconnectUnlocked(i int, from []int) {
	// log.Printf("disconnect peer %d from %v\n", i, from)

	// outgoing socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][from[j]]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[from[j]][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}

func (cfg *config) All() []int {
	all := make([]int, cfg.n)
	for i := 0; i < cfg.n; i++ {
		all[i] = i
	}
	return all
}

func (cfg *config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}

// Sets up 2 partitions with connectivity between servers in each  partition.
func (cfg *config) partition(p1 []int, p2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// log.Printf("partition servers into: %v %v\n", p1, p2)
	for i := 0; i < len(p1); i++ {
		cfg.disconnectUnlocked(p1[i], p2)
		cfg.connectUnlocked(p1[i], p1)
	}
	for i := 0; i < len(p2); i++ {
		cfg.disconnectUnlocked(p2[i], p1)
		cfg.connectUnlocked(p2[i], p2)
	}
}

// Create a clerk with clerk specific server names.
// Give it connections to all of the servers, but for
// now enable only connections to servers in to[].
func (cfg *config) makeClient(to []int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh set of ClientEnds.
	ends := make([]*rpc.ClientEnd, cfg.n)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], j)
	}

	ck := MakeClerk(random_handles(ends))
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	cfg.ConnectClientUnlocked(ck, to)
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}

// caller should hold cfg.mu
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	// log.Printf("ConnectClient %v to %v\n", ck, to)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]
		cfg.net.Enable(s, true)
	}
}

func (cfg *config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}

// caller should hold cfg.mu
func (cfg *config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	// log.Printf("DisconnectClient %v from %v\n", ck, from)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(from); j++ {
		s := endnames[from[j]]
		cfg.net.Enable(s, false)
	}
}

func (cfg *config) DisconnectClient(ck *Clerk, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.DisconnectClientUnlocked(ck, from)
}

// 干掉服务器i
func (cfg *config) ShutdownServer(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, cfg.All())
	// 从网络中删除
	cfg.net.DeleteServer(i)

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if cfg.saved[i] != nil {
		cfg.saved[i] = cfg.saved[i].Copy()
	}

	kv := cfg.kvservers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		cfg.kvservers[i] = nil
	}
}

// If restart servers, first call ShutdownServer
func (cfg *config) StartServer(i int) {
	cfg.mu.Lock()
	// 互斥锁保护状态信息的变更
	// 为服务器i生成到每个服务器的连接的客户端
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}
	// 将所有客户端添加在网络中
	ends := make([]*rpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}

	if cfg.saved[i] != nil {
		// 读取持久化内容
		cfg.saved[i] = cfg.saved[i].Copy()
	} else {
		// 创建持久化文件
		cfg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	// 启动服务器
	cfg.kvservers[i] = StartKVServer(ends, i, cfg.saved[i], cfg.maxraftstate)

	// 创建service
	kvsvc := rpc.MakeService(cfg.kvservers[i])
	rfsvc := rpc.MakeService(cfg.kvservers[i].rf)
	// 创建服务器，并把服务添加到服务器中
	srv := rpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	// 向网络中添加服务器
	cfg.net.AddServer(i, srv)
}

// 获取leader信息
func (cfg *config) Leader() (bool, int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	for i := 0; i < cfg.n; i++ {
		_, isLeader := cfg.kvservers[i].rf.GetState()
		if isLeader {
			return true, i
		}
	}
	return false, 0
}

// Partition servers into 2 groups and put current leader in minority
func (cfg *config) make_partition() ([]int, []int) {
	_, leader := cfg.Leader()
	p1 := make([]int, cfg.n/2+1)
	p2 := make([]int, cfg.n/2)
	j := 0
	// p1[0,1,...,n/2]
	// p2[n/2,..,leader]
	for i := 0; i < cfg.n; i++ {
		if i != leader {
			if j < len(p1) {
				p1[j] = i
			} else {
				p2[j-len(p1)] = i
			}
			j++
		}
	}
	p2[len(p2)-1] = leader
	return p1, p2
}

// 创建配置中心
func make_config(t *testing.T, tag string, n int, unreliable bool, maxraftstate int) *config {
	runtime.GOMAXPROCS(4)
	cfg := &config{
		t:            t,
		tag:          tag,
		net:          rpc.MakeNetwork(),
		n:            n,
		kvservers:    make([]*RaftKV, n),
		saved:        make([]*raft.Persister, n),
		endnames:     make([][]string, n),
		clerks:       make(map[*Clerk][]string),
		nextClientId: n + 1000, // client ids start 1000 above the highest serverid
		maxraftstate: maxraftstate,
	}

	// 启动所有服务器
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(i)
	}

	cfg.ConnectAll()

	cfg.net.Reliable(!unreliable)

	return cfg
}
