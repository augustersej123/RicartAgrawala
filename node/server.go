// Nicklas
package main

import (
	"fmt"
	"log"
	"net"

	pb "github.com/augustersej123/RicartAgrawala/proto"
	"google.golang.org/grpc"
)

type server struct {
	gRPCServer *grpc.Server
}

func startServer(address string, node *Node) (*server, error) {
	//net.listen creates a network listener, and tcp is using the tcp protocol,
	// and address is the address to listen on

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %v", err)
	}

	// Register the server with the gRPC server
	grpcServer := grpc.NewServer()

	// Register Node as the gRPC service implementation.
	pb.RegisterRicartAgrawalaServer(grpcServer, node)

	s := &server{
		gRPCServer: grpcServer,
	}

	// Start serving incoming requests in the background.
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	log.Printf("gRPC server listening on %s", address)

	return s, nil
}

func (s *server) stopServer() {
	s.gRPCServer.GracefulStop()
	//This makes the server stop accepting new request,
	//but stil finish processing the currently running requests before shutting down.
	log.Println("gRPC server stopped")
}
