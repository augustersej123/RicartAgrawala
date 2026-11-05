// Nicklas
package main

import (
	"fmt"
	"log"
	"pb"

)

type client struct {
	peer.Map[int]pb.mutualExclusionClient
}

func ConnectToPeers(adress string) (pb.mutualExclusionClient, error) {

}

func (c *client) BroadcastRequest (
	id int, 
	lamport int64
) 

func (c *client) BroadcastRequest(
	peerId int, 
	request *pb.Request,
)

func (c *Client) sendRequest(
	peerId int,
	request *pb.Request,
) 
