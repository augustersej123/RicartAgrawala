// Vitus
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	pb "github.com/augustersej123/RicartAgrawala/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type PeerAddress struct {
	NodeID  int64
	Address string
}

type Peer struct {
	NodeID int64
	client pb.RicartAgrawalaClient
	conn   *grpc.ClientConn
}

type Peers struct {
	localID int64
	byID    map[int64]*Peer
}

func NewPeers(localID int64, nodes []PeerAddress) (*Peers, error) {
	peers := &Peers{
		localID: localID,
		byID:    make(map[int64]*Peer),
	}
	seen := make(map[int64]bool)
	foundLocal := false

	for _, node := range nodes {
		if seen[node.NodeID] {
			peers.Close()
			return nil, fmt.Errorf("duplicate node ID %d", node.NodeID)
		}
		seen[node.NodeID] = true

		if node.NodeID == localID {
			foundLocal = true
			continue // A node does not send requests to itself.
		}
		if node.Address == "" {
			peers.Close()
			return nil, fmt.Errorf("node %d has no address", node.NodeID)
		}

		conn, err := grpc.NewClient(
			node.Address,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			peers.Close()
			return nil, fmt.Errorf("connect to node %d: %w", node.NodeID, err)
		}

		peers.byID[node.NodeID] = &Peer{
			NodeID: node.NodeID,
			client: pb.NewRicartAgrawalaClient(conn),
			conn:   conn,
		}
	}

	if !foundLocal {
		peers.Close()
		return nil, fmt.Errorf("node ID %d is missing from the node list", localID)
	}
	if len(peers.byID) == 0 {
		return nil, fmt.Errorf("node %d has no peers", localID)
	}

	return peers, nil
}

// RequestAll sends a request to every other node concurrently.
// It returns nil only after every peer's Request RPC has returned a Reply.
func (p *Peers) RequestAll(ctx context.Context, timestamp int64) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(p.byID))

	for _, peer := range p.byID {
		wg.Add(1)

		go func(peer *Peer) {
			defer wg.Done()

			log.Printf(
				"[Node %d] event=request_send to=%d timestamp=%d",
				p.localID,
				peer.NodeID,
				timestamp,
			)

			reply, err := peer.client.Request(ctx, &pb.RequestMessage{
				Timestamp: timestamp,
				NodeID:    p.localID,
			})

			if err != nil {
				errs <- fmt.Errorf("request permission from node %d: %w", peer.NodeID, err)
				return
			}
			log.Printf(
				"[Node %d] event=reply_received from=%d",
				p.localID,
				reply.NodeID,
			)
		}(peer)

	}

	wg.Wait()
	close(errs)

	for err := range errs {
		return err
	}
	return nil
}

func (p *Peers) Close() {
	for _, peer := range p.byID {
		_ = peer.conn.Close()
	}
}
