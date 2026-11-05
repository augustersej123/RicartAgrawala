// Nicklas
package main

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"RicartAgrawala/proto"
)

type client struct {
	peers map[int]proto.RicartAgrawalaClient
	conns map[int]*grpc.ClientConn
}

// needs to dial another node, create a gRPC client and return it
func ConnectToPeers(adress string) (
	proto.RicartAgrawalaClient,
	*grpc.ClientConn,
	error,
) {

	//Create a gRPC connection
	conn, err := grpc.NewClient(
		adress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		return nil, err
	}

	//Creates a new client using previously created connection
	//peer can now use rpc methods defined in the proto file
	peer := proto.newRicartAgrawalaClient(conn)

	return peer, conn, nil
}

//in broadcastRequest, we will send a request to all peers
// agrawala's algorithm requires that we send a request to all peers when we
// want to enter the critical section
//this function collects and counts the replies and returns an error
// if any of the requests fail

func (c *client) BroadcastRequest(
	request *proto.Request,
) error {
	// iterates over all peers
	for peerId, peer := range c.peers {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		//Send request to the current peer
		_, err := peer.Request(ctx, request)
	}
}

// send a request to a specific peer and return the reply
// Request contains the id and timestamp of the requesting node
// peer will decide on whether to reply or defer the reply based on its own request and timestamp
func (c *client) sendRequest(
	ctx context.Context,
	peerId int,
	request *proto.RequestMessage,

) (*proto.Reply, error) {

	peer, exists := c.peers[peerId]
	if !exists {
		return nil, fmt.Errorf("peer with ID %d not found", peerId)
	}
	reply, err := peer.Request(ctx, request)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to send request to peer %d: %w",
			peerId,
			err)
	}
	return reply, nil
}
