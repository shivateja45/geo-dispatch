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
	TotalDrivers   = 5000
	ServerAddress  = "localhost:9000"
	UpdateInterval = 3 * time.Second
)

func main() {
	var wg sync.WaitGroup
	wg.Add(TotalDrivers)

	log.Printf("🚀 Starting load test: %d drivers -> %s", TotalDrivers, ServerAddress)

	for i := 0; i < TotalDrivers; i++ {
		go simulateDriver(i, &wg)

		// Rate limit connection creation to prevent file descriptor exhaustion
		if i%100 == 0 {
			time.Sleep(50 * time.Millisecond)
			fmt.Printf("Drivers active: %d\n", i)
		}
	}

	wg.Wait()
}

func simulateDriver(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", ServerAddress)
	if err != nil {
		// log.Printf("Connection failed for driver %d", id)
		return
	}
	defer conn.Close()

	// Initial position (San Francisco)
	lat := 37.7749 + (rand.Float64() * 0.1)
	lon := -122.4194 + (rand.Float64() * 0.1)

	for {
		// Random walk
		lat += (rand.Float64() - 0.5) * 0.001
		lon += (rand.Float64() - 0.5) * 0.001

		// Protocol: Line-delimited JSON
		payload := fmt.Sprintf(`{"driver_id": "driver-%d", "latitude": %.6f, "longitude": %.6f}`+"\n", id, lat, lon)

		if _, err := conn.Write([]byte(payload)); err != nil {
			return
		}

		time.Sleep(UpdateInterval + time.Duration(rand.Intn(1000))*time.Millisecond)
	}
}
