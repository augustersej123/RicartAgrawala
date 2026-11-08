package main

import (
	"fmt"
	"log"
	"time"
)

func criticalSection(id int, lamport int64) {
	// Implementation for critical section

	// Log entry into the critical section.
	log.Printf("[Node %d] event=cs_enter lamport=%d", id, lamport)

	fmt.Printf("Node %d is in the critical section\n", id)

	// Simulate some work while holding the critical section.
	time.Sleep(2 * time.Second)

	// Log exit from the critical section.
	log.Printf("[Node %d] event=cs_exit lamport=%d", id, lamport)
	fmt.Printf("Node %d is leaving the critical section\n", id)

}
