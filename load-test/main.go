package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	TotalDrivers   = 5000 // Let's start with 5,000 to be safe on a laptop
	ServerAddress  = "localhost:9000"
	UpdateInterval = 3 * time.Second
)

func main() {
	var wg sync.WaitGroup
	wg.Add(TotalDrivers)

	log.Printf("🚀 Starting load test with %d drivers...", TotalDrivers)

	// We launch drivers in batches to avoid opening too many files at once instantly
	for i := 0; i < TotalDrivers; i++ {
		go simulateDriver(i, &wg)

		// Small delay every 100 drivers to ramp up smoothly
		if i%100 == 0 {
			time.Sleep(100 * time.Millisecond)
			fmt.Printf("Launched %d drivers...\n", i)
		}
	}

	wg.Wait() // Wait forever (or until drivers crash)
}

func simulateDriver(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	// 1. Connect to the Rust Server
	conn, err := net.Dial("tcp", ServerAddress)
	if err != nil {
		// If connection fails (e.g., server full), just log and exit this driver
		// log.Printf("Driver %d failed to connect: %v", id, err)
		return
	}
	defer conn.Close()

	// Generate a random starting point (roughly near San Francisco)
	lat := 37.7749 + (rand.Float64() * 0.1)
	lon := -122.4194 + (rand.Float64() * 0.1)

	// 2. Start the update loop
	for {
		// Move the driver slightly (random walk)
		lat += (rand.Float64() - 0.5) * 0.001
		lon += (rand.Float64() - 0.5) * 0.001

		// Create the JSON payload
		// Note: We add a newline \n because our Rust server expects line-by-line reading
		payload := fmt.Sprintf(`{"driver_id": "driver-%d", "latitude": %.6f, "longitude": %.6f}`+"\n", id, lat, lon)

		// 3. Send data
		_, err := conn.Write([]byte(payload))
		if err != nil {
			log.Printf("Driver %d lost connection", id)
			return
		}

		// 4. Sleep before next update
		time.Sleep(UpdateInterval + time.Duration(rand.Intn(1000))*time.Millisecond)
	}
}
