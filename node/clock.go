// Vitus
package main

import "sync"

// Clock is a Lamport clock.
type Clock struct {
	mu   sync.Mutex // protects time
	time int64
}

// Tick is for local events: time = time + 1
func (c *Clock) Tick() int64 { // Tick is for events caused by the server itself, like a leave caused by a dropped connection. It increments the clock and returns the new time.
	c.mu.Lock()         // Lock the mutex to ensure two goroutines don't update the clock at the same time, which would cause a race condition.
	defer c.mu.Unlock() // Unlock the mutex when the function returns
	c.time++
	return c.time
}

// Update is for receiving events: time = max(local, received) + 1
func (c *Clock) Update(received int64) int64 { //Used when a message is received from another participant.
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.time {
		c.time = received
	}
	c.time++
	return c.time
}

// Now reads the time without changing it.
func (c *Clock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}
