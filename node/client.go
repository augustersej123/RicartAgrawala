// Nicklas
package main

import (
	"fmt"
	"log"
	"google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"

    "RicartAgrawala/proto"
)

type client struct {
	peers map[int]proto.RicartAgrawalaClient
	conns map[int]*grpc.ClientConn
}

//needs to dial another node, create a gRPC client and return it
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

func (c *client) BroadcastRequest (
	id int, 
	lamport int64
) 


func (c *Client) sendRequest(
	peerId int,
	request *pb.Request,
) 
