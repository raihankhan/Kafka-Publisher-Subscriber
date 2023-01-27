package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {

	fmt.Printf("producer starting ........\n")

	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	topic := os.Getenv("TOPIC")
	clientID := strings.Join([]string{topic, "producer", string(rand.Intn(4))}, "-")

	// Create a new Kafka producer config
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"client.id":         clientID,
	}

	// Create a new Kafka producer
	p, err := kafka.NewProducer(config)
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return
	}

	fmt.Printf("Producer client created .......\n")

	defer func() {
		p.Flush(10000)
		p.Close()
		fmt.Printf("Kafka producer client closed ......\n")
	}()

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	// Send messages to the "demo" topic every 3 seconds
	for {
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte("data"),
			Value:          []byte(fmt.Sprintf("random event %d", rand.Intn(1000))),
		}
		_ = p.Produce(message, nil)
		time.Sleep(5 * time.Second)
	}
}
