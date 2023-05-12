package main

import (
	"context"
	"fmt"
	"github.com/a-agmon/redis-streams-wrapper/lib"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	numberOfMessages := 1000000
	numberOfConsumers := 20
	redisClient := initRedisClient("localhost:6379", "")
	for i := 0; i < numberOfMessages; i++ {
		err := redisClient.ProduceMessage(context.Background(), "test-stream-100",
			map[string]interface{}{"MessageNumber": fmt.Sprintf("%d/%d", i, numberOfMessages)})
		if err != nil {
			log.Printf("Error producing message: %v", err)
		}
	}
	for i := 0; i < numberOfConsumers; i++ {
		err := createPollingConsumer("localhost:6379", "test-stream-100", "test-group-100", fmt.Sprintf("consumer%d", i))
		if err != nil {
			log.Printf("Error creating polling consumer: %v", err)
		}
	}

	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-exitChannel //block until we get a signal

}

func initRedisClient(redisURL string, consumerName string) *lib.RedisStreamsClient {
	redisClient := lib.NewRedisClient(lib.RedisClientConfig{
		Addr:     redisURL,
		Username: "",
		Password: "",
		DB:       0,
	}, consumerName)
	return redisClient
}

func createPollingConsumer(redisURL string, stream string, consumerGroup string, consumerName string) error {
	redisClient := initRedisClient(redisURL, consumerName)
	ctx := context.Background()
	// Create a consumer group if it does not exist
	err := redisClient.CreateConsumerGroupIfNotExists(ctx, stream, consumerGroup)
	if err != nil {
		return fmt.Errorf("error creating consumer group: %v", err)
	}
	// Start polling routine for messages
	go func() {
		log.Printf("consumer %s started polling for messages", consumerName)
		for {
			messages, err := redisClient.FetchNewMessages(ctx, stream, consumerGroup, 100, 3)
			if err != nil {
				log.Printf("error fetching messages: %v", err)
			}
			for _, msg := range messages {
				log.Printf("message: %v", msg)
				err := redisClient.AckMessage(ctx, msg.StreamName, msg.ConsumerName, msg.ID)
				if err != nil {
					return
				}
			}
		}
	}()
	return nil
}
