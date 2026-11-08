//Nicklas
package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"net"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/augustersej123/RicartAgrawala/proto";
)

type server struct {
	proto.UnimplementedRicartAgrawalaServer

	gRPCServer *grpc.Server
	peer *peer

}

func startServer(address string) (*server, error) {
	// Implementation for starting the server

	grpcServer := gRPC.NewServer()

	//net.listen creates a network listener, and tcp is using the tcp protocol, 
	// and address is the address to listen on
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	// Register the server with the gRPC server
	proto.RegisterRicartAgrawalaServer(grpcServer, &server{})

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("gRPC server stopped %v", err)
		}
	}()
	


	return &server{}, nil
}
//Here we take the request, and find the timestamp and nodeID of the request, and 
// then we can use that information to determine if we should send a reply or not.
//The returning logic is left up to peer.go

func (s *server) Request (
	ctx context.Context,
	req *proto.RequestMessage,
) (*proto.Reply, error){

	timeStamp := req.timestamp()	
	nodeID := req.NodeID

}



func (s *server) handleRequest( timestamp int64, nodeID int) <-chan struct{}  {

	
}

func (s *server) SendReply {

}

func (s *server) stopServer() {

}