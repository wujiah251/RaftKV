# Raft

这是为了学习分布式系统所做的Raft一致性协议demo

目录：
```
root
    \raft
        config.go
        log.go
        persister.go
        raft.go
        rpc.go
        runtime.go
        timer.go
        utils.go
    \rpc
        rpc.go
```

## 关键

互斥锁的使用。
raft节点的互斥锁主要保护的状态：节点类型