package main

import (
	"context"
	"fmt"
	"log"
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
	peers *Peers

	mu        sync.Mutex
	state     State
	requestTS int64 //Timestamp of the request

	deferred map[int64]chan struct{} //Map of deferred replies, key is the node ID, value is a channel to signal when the reply can be sent
}

func NewNode(id int64, peers *Peers) *Node {

	n := &Node{
		id:       id,
		clock:    &Clock{},
		peers:    peers,
		state:    Released,
		deferred: make(map[int64]chan struct{}),
	}

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

	lamport := n.clock.Update(ts)

	n.mu.Lock()
	defer n.mu.Unlock()

	replyReady := make(chan struct{})

	if n.shouldDefer(ts, requestingNodeID) {

		n.deferred[requestingNodeID] = replyReady
		log.Printf(
			"[Node %d] event=defer from=%d req_ts=%d state=%s lamport=%d",
			n.id, requestingNodeID, ts, n.state, lamport,
		)
	} else {
		close(replyReady)
		log.Printf("[Node %d] event=reply from=%d req_ts=%d state=%s lamport=%d",
			n.id, requestingNodeID, ts, n.state, lamport)
	}
	return replyReady
}

func (n *Node) Request(
	ctx context.Context,
	req *pb.RequestMessage,
) (*pb.Reply, error) {

	//pass time and nodeid to HandleRequest, and handleRequest
	// will determine if reply can be send now or later
	replyReady := n.HandleRequest(req.Timestamp, req.NodeID)

	select {
	//if channel closes, we send reply, if context is done, we return error
	case <-replyReady:
		return &pb.Reply{
			NodeID: n.id,
		}, nil
	case <-ctx.Done():
		n.cancelDeferredReply(req.NodeID, replyReady)

		return nil, ctx.Err()

	}
}

func (n *Node) cancelDeferredReply(
	nodeID int64,
	replyReady <-chan struct{},

) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if ch, exists := n.deferred[nodeID]; exists {
		if ch == replyReady {
			delete(n.deferred, nodeID)
		}
	}
}

func (n *Node) LeaveCriticalSection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.state = Released
	for nodeID, ch := range n.deferred {
		close(ch)
		delete(n.deferred, nodeID)
	}
}

func (n *Node) EnterCriticalSection(ctx context.Context) error {
	timestamp := n.clock.Tick()

	n.mu.Lock()

	if n.state != Released {
		state := n.state
		n.mu.Unlock()

		return fmt.Errorf("node %d cannot request critical section while %s",
			n.id, state)
	}

	n.requestTS = timestamp
	n.state = Wanted

	n.mu.Unlock()

	// Ask all other nodes for permission.
	if err := n.peers.RequestAll(ctx, timestamp); err != nil {
		n.mu.Lock()
		n.state = Released
		n.mu.Unlock()

		return err
	}

	// All permissions received: enter the critical section.
	n.mu.Lock()
	n.state = Held
	n.mu.Unlock()

	return nil
}
