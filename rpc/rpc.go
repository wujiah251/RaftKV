package rpc

import (
	"bytes"
	"encoding/gob"
	"log"
	"reflect"
)

type ClientEnd struct {
	endName interface{}
	ch      chan reqMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type reqMsg struct {
	endName  interface{} // name of sending ClientEnd
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

func (c *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endName = c.endName
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	// TODO:不懂怎么搞
	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	qe.Encode(args)
	req.args = qb.Bytes()

	c.ch <- req

	// 获得响应
	resp := <-req.replyCh
	if resp.ok {
		rb := bytes.NewBuffer(resp.reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}
