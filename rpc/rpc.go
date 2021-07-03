package rpc

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"
)

// RPC请求的消息格式
type reqMsg struct {
	endname  interface{}   // 客户端名称
	svcMeth  string        // "服务名称.方法名称"，e.g. "Raft.AppendEntries"
	argsType reflect.Type  // 参数类型
	args     []byte        // 参数
	replyCh  chan replyMsg // 用于传回响应
}

type replyMsg struct {
	ok    bool
	reply []byte
}

// 客户端，定义如下：
type ClientEnd struct {
	endname interface{} // 客户端名称
	ch      chan reqMsg // 发送消息的模拟网络，和NetWork的endCh是同一个channel
}

// client调用RPC过程
func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	// 初始化一个req
	req := reqMsg{}
	req.endname = e.endname
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	// replyCh类似于一个socket
	req.replyCh = make(chan replyMsg)

	// 序列化
	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	// 发送请求到模拟网络中
	e.ch <- req

	// 等待Server端口返回数据（可能乱序）
	rep := <-req.replyCh

	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu             sync.Mutex                  // 互斥锁
	reliable       bool                        // 用来模拟网络是否可靠
	longDelays     bool                        // 模拟慢网络
	longReordering bool                        // 模拟网络乱序
	ends           map[interface{}]*ClientEnd  // 网络中所有client
	enabled        map[interface{}]bool        // 模拟server是否宕机
	servers        map[interface{}]*Server     // servers, by name
	connections    map[interface{}]interface{} // client到server的所有连接
	endCh          chan reqMsg                 // 用来模拟传送数据的网络
}

func MakeNetwork() *Network {
	rn := &Network{}
	rn.reliable = true
	rn.ends = map[interface{}]*ClientEnd{}
	rn.enabled = map[interface{}]bool{}
	rn.servers = map[interface{}]*Server{}
	rn.connections = map[interface{}](interface{}){}
	rn.endCh = make(chan reqMsg)

	// single goroutine to handle all ClientEnd.Call()s
	go func() {
		// 网络处理流程
		for xreq := range rn.endCh {
			go rn.ProcessReq(xreq)
		}
	}()

	return rn
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.reliable = yes
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longReordering = yes
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.longDelays = yes
}

// 返回客户端信息，包括客户端、连接的服务器、服务器实例、网络是否可靠（延迟、丢包）、网络是否乱序
func (rn *Network) ReadEndnameInfo(endname interface{}) (enabled bool,
	servername interface{}, server *Server, reliable bool, longreordering bool,
) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

// 判断服务器是否挂掉了
func (rn *Network) IsServerDead(endname interface{}, servername interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.enabled[endname] == false || rn.servers[servername] != server {
		return true
	}
	return false
}

// 网络处理请求
func (rn *Network) ProcessReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.ReadEndnameInfo(req.endname)
	if enabled && servername != nil && server != nil {
		if reliable == false {
			// short delay
			ms := (rand.Int() % 27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		if reliable == false && (rand.Int()%1000) < 100 {
			// 1/10的概率丢弃消息
			req.replyCh <- replyMsg{false, nil}
			return
		}
		// 只想请求：调用RPC handler
		ech := make(chan replyMsg)
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		var reply replyMsg
		replyOK := false
		serverDead := false
		for replyOK == false && serverDead == false {
			select {
			case reply = <-ech:
				replyOK = true // 成功获得响应
			case <-time.After(100 * time.Millisecond):
				// 获取服务器是否dead的信息
				serverDead = rn.IsServerDead(req.endname, servername, server)
			}
		}

		serverDead = rn.IsServerDead(req.endname, servername, server)

		if replyOK == false || serverDead == true {
			// server was killed while we were waiting; return error.
			// 服务器挂掉了，则返回空reply
			req.replyCh <- replyMsg{false, nil}
		} else if reliable == false && (rand.Int()%1000) < 100 {
			// 如果网络不可靠，以一定概率丢失reply（即返回一个空reply）
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering == true && rand.Intn(900) < 600 {
			// delay the response for a while
			// 如果模拟乱序，则以一定概率等待返回
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.Sleep(time.Duration(ms) * time.Millisecond)
			req.replyCh <- reply
		} else {
			req.replyCh <- reply
		}
	} else {
		// simulate no reply and eventual timeout.
		ms := 0
		if rn.longDelays {
			// let Raft tests check that leader doesn't send
			// RPCs synchronously.
			ms = (rand.Int() % 7000)
		} else {
			// many kv tests require the client to try each
			// server in fairly rapid succession.
			ms = (rand.Int() % 100)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		req.replyCh <- replyMsg{false, nil}
	}

}

// 创建一个Client
// 把enabled和connection设置成空
func (rn *Network) MakeEnd(endname interface{}) *ClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if _, ok := rn.ends[endname]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endname)
	}

	e := &ClientEnd{}
	e.endname = endname
	e.ch = rn.endCh
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil

	return e
}

// 向网络中添加一个服务器
func (rn *Network) AddServer(servername interface{}, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = rs
}

// 从网络中删除一个服务器
func (rn *Network) DeleteServer(servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.servers[servername] = nil
}

// 连接一个客户端到服务器
func (rn *Network) Connect(endname interface{}, servername interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.connections[endname] = servername
}

// 设置client对应的server是否宕机
func (rn *Network) Enable(endname interface{}, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.enabled[endname] = enabled
}

// get a server's count of incoming RPCs.
func (rn *Network) GetCount(servername interface{}) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	svr := rn.servers[servername]
	return svr.GetCount()
}

//
// a server is a collection of services, all sharing
// the same rpc dispatcher. so that e.g. both a Raft
// and a k/v server can listen to the same rpc endpoint.
//

// 服务器：包含一些列服务
type Server struct {
	mu       sync.Mutex
	services map[string]*Service // 包含的所有服务
	count    int                 // 到达Server的送RPC数量
}

// 创建一个Server
func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

// 想server中添加service
func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

// 服务器处理请求
func (rs *Server) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()
	// RPC调用数量+1
	rs.count += 1

	// 分离服务名和方法名
	// split Raft.AppendEntries into service and method
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	// 找到对应的服务
	service, ok := rs.services[serviceName]

	rs.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		// 没有找到对应的服务
		choices := []string{}
		for k, _ := range rs.services {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Server.dispatch(): unknown service %v in %v.%v; expecting one of %v\n",
			serviceName, serviceName, methodName, choices)
		return replyMsg{false, nil}
	}
}

// 获取服务器RPC的数量
func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

// service上定义了一系列的方法，每个方法对应RPC的一个调用函数。整个处理流程如下：
type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method // 服务
}

// 创建一个service
func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	svc.methods = map[string]reflect.Method{}

	// 通过reflect.TypeOf(rcvr).NumMethod来获取所有方法
	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		// 将合法的方法添加到service中
		if method.PkgPath != "" || // capitalized?
			mtype.NumIn() != 3 ||
			//mtype.In(1).Kind() != reflect.Ptr ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0 {
			// the method is not suitable for a handler
			// fmt.Printf("bad method: %v\n", mname)
		} else {
			// the method looks like a handler
			svc.methods[mname] = method
		}
	}

	return svc
}

// 解析方法和请求
func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		// prepare space into which to read the argument.
		// the Value's type will be a pointer to req.argsType.
		args := reflect.New(req.argsType)

		// 解析参数
		ab := bytes.NewBuffer(req.args)
		ad := gob.NewDecoder(ab)
		ad.Decode(args.Interface())

		// allocate space for the reply.（为reply分配空间）
		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		// 调用方法
		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		// 响应进行序列化
		rb := new(bytes.Buffer)
		re := gob.NewEncoder(rb)
		re.EncodeValue(replyv)
		// 返回结构
		return replyMsg{true, rb.Bytes()}
	} else {
		choices := []string{}
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("labrpc.Service.dispatch(): unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{false, nil}
	}
}
