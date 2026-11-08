package main

import (
	"log"
)

func main() {

	_, err := startServer("localhost:50051")
	if err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	log.Println("Server started on %s", "localhost:50051")

	select {}

}
