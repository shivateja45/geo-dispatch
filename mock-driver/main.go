package main

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	// 1. Create a new Producer
	// We're giving it the same "address" for the Kafka broker.
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v\n", err)
	}
	defer p.Close() // Defer the close for cleanup

	// This is the topic we want to send messages to.
	topic := "driver-locations"

	log.Println("Mock driver started. Sending messages...")

	// 2. Start an infinite loop to send messages
	for {
		// This is our sample driver location message.
		// In a real app, this would be a real lat/lon.
		message := `{"driver_id": "123", "latitude": 40.7128, "longitude": -74.0060}`

		// 3. Send the message
		// We create a new "message" object and send it.
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny, // Let Kafka decide which partition
			},
			Value: []byte(message), // Send the message as raw bytes
		}, nil) // 'nil' means we're not handling delivery reports right now

		// Print what we just did.
		log.Printf("Sent message: %s\n", message)

		// Wait for 3 seconds before sending the next one.
		time.Sleep(3 * time.Second)
	}
}
