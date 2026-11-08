package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	nodeID := flag.Int64("id", 1, "ID of this node")
	requestCS := flag.Bool("request", false, "Request the Critical Section on startup")
	flag.Parse()

	nodes := []PeerAddress{
		{NodeID: 1, Address: "localhost:5001"},
		{NodeID: 2, Address: "localhost:5002"},
		{NodeID: 3, Address: "localhost:5003"},
	}

	var address string
	found := false

	for _, n := range nodes {
		if n.NodeID == *nodeID {
			address = n.Address
			found = true
			break
		}
	}

	if !found {
		log.Fatalf("Unknown node ID: %d", *nodeID)
	}

	peers, err := NewPeers(*nodeID, nodes)
	if err != nil {
		log.Fatal(err)
	}
	defer peers.Close()

	node := NewNode(*nodeID, peers)

	server, err := startServer(address, node)

	if err != nil {
		log.Fatal(err)
	}

	// Request the Critical Section only if the flag is enabled.
	if *requestCS {
		go func() {
			time.Sleep(2 * time.Second)

			ctx, cancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			defer cancel()

			log.Printf("[Node %d] Requesting Critical Section", *nodeID)

			if err := node.EnterCriticalSection(ctx); err != nil {
				log.Printf("[Node %d] Failed to enter Critical Section: %v", *nodeID, err)
				return
			}

			criticalSection(int(*nodeID), node.clock.Now())
			node.LeaveCriticalSection()

			log.Printf("[Node %d] Released Critical Section", *nodeID)
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("Shutting down...")
	server.stopServer()

}
