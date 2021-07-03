package raftkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrorWrongLeader"
	Put            = "Put"
	Append         = "Append"
	PutType        = 0
	AppendType     = 1
	GetType        = 2
)

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	Id       int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         string
}

type GetArgs struct {
	Key      string
	Value    string
	ClientId int64
	Id       int64
}

type GetReply struct {
	WrongLeader bool
	Err         string
	Value       string
}
