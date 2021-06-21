package raft

import (
	"fmt"
	"log"
)

const Debug = 0

func (r *Raft) ToString() string {
	return fmt.Sprintf("LID: %v;Term:%d;commitIndex:%v;",
		r.leaderId, r.currentTerm, r.committedIndex)
}

func (r *Raft) GetRole() string {
	var role string
	switch r.role {
	case LEADER:
		role = "Lead"
	case FOLLOWER:
		role = "Foll"
	case CANDIDATE:
		role = "Cand"
	}
	//return role + " " + rf.toStringWithoutLog()
	return role + " " + r.ToString()
	//return role
}

func DEBUG(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
}
