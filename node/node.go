package main

import (
	"sync"

	pb "github.com/augustersej123/RicartAgrawala/proto"
)

type State int

const (
	Released State = iota
	Wanted
	Held
)

func (s State) String() string {
	switch s {
	case Released:
		return "RELEASED"
	case Wanted:
		return "WANTED"
	case Held:
		return "HELD"
	default:
		return "UNKNOWN"
	}
}

type Node struct {
	pb.UnimplementedRicartAgrawalaServer

	id    int64
	clock *LamportClock
	peers map[int64]pb.RicartAgrawalaClient

	mu        sync.Mutex
	cond      *sync.Cond
	state     State
	requestTS int64 //Timestamp of the request
}

func NewNode(id int64) *Node {
	n := &Node{
		id:    id,
		clock: NewLamportClock(),
		peers: make(map[int64]pb.RicartAgrawalaClient),
		state: Released,
	}
	n.cond = sync.NewCond(&n.mu)
	return n
}
