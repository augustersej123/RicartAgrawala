package main

import (
	"fmt"
	"log"
	"time"
)

func criticalSection(id int, lamport int64) {
	// Implementation for critical section
	log.Printf("[Node %d] event=cs_enter lamport=%d", id, lamport)

	fmt.Printf("Node %s is in the critical section\n", id)

	time.Sleep(2 * time.Second) // Simulate some work in the critical section

	log.Printf("[Node %d] event=cs_exit lamport=%d", id, lamport)
	fmt.Printf("Node %s is leaving the critical section\n", id)

}
