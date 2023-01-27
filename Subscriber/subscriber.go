package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {

	fmt.Printf("Run 3\n")

	fmt.Printf("Starting consumer ...............\n")

	rand.Seed(34)
	bootstrapServers := os.Getenv("BOOTSTRAP_SERVERS")
	topic := os.Getenv("TOPIC")
	clientID := strings.Join([]string{topic, "subscriber", string(rand.Intn(1000))}, "-")

	// Create a new Kafka consumer config
	config := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          clientID,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"session.timeout.ms":    time.Second * 10,
		"auto.offset.reset":     "earliest",
	}

	// Create a new Kafka consumer
	c, err := kafka.NewConsumer(config)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s\n", err)
		return
	}

	fmt.Printf("Consumer client created ..........\n")

	defer func() {
		err := c.Unsubscribe()
		if err != nil {
			return
		}
		err = c.Close()
		if err != nil {
			return
		}
		fmt.Printf("Kafka consumer client closed ......\n")
	}()

	err = c.Subscribe(topic, nil)
	if err != nil {
		fmt.Printf("Failed to Subscribe to topic %s", topic)
		return
	}
	fmt.Printf("Subscribed to topic %s", topic)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(int(time.Second * 5))
			if ev == nil {
				fmt.Printf("Consumer poll timeout")
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n",
					e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n",
						e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored event %v\n", e)
			}
		}
	}
}
