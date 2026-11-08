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
	clock *Clock
	peers map[int64]pb.RicartAgrawalaClient

	mu        sync.Mutex
	cond      *sync.Cond
	state     State
	requestTS int64 //Timestamp of the request

	deferred map[int64]chan struct{} //Map of deferred replies, key is the node ID, value is a channel to signal when the reply can be sent
}

func NewNode(id int64) *Node {

	n := &Node{
		id:       id,
		clock:    &Clock{},
		peers:    make(map[int64]pb.RicartAgrawalaClient),
		state:    Released,
		deferred: make(map[int64]chan struct{}),
	}
	n.cond = sync.NewCond(&n.mu)
	return n
}

// hasPriority reports our own request comes before (ts, id)
// lower timestamp wins, node ID breaks ties. Caller must hold n.mu

func (n *Node) hasPriority(ts, id int64) bool {
	if n.requestTS != ts {
		return n.requestTS < ts
	}
	return n.id < id
}

// shouldDefer reports whether a reply to (ts, id) must wait. Caller must hold n.mu
func (n *Node) shouldDefer(ts, id int64) bool {
	return n.state == Held || (n.state == Wanted && n.hasPriority(ts, id))
}

func (n *Node) HandleRequest(ts, requestingNodeID int64) <-chan struct{} {
	n.clock.Update(ts)

	n.mu.Lock()
	defer n.mu.Unlock()

	replyReady := make(chan struct{})

	if n.shouldDefer(ts, requestingNodeID) {
		n.deferred[requestingNodeID] = replyReady

	} else {
		close(replyReady)
	}
	return replyReady
}
