package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	rediswrapper "github.com/a-agmon/redis-streams-wrapper/v1"
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

func initRedisClient(redisURL string, consumerName string) *rediswrapper.RedisStreamsClient {
	redisClient := rediswrapper.NewRedisClientWrapper(rediswrapper.RedisClientConfig{
		Addr:         redisURL,
		Username:     "",
		Password:     "",
		DB:           0,
		ConsumerName: consumerName,
	})
	return redisClient
}

func createPollingConsumer(redisURL string, stream string, consumerGroup string, consumerName string) error {
	redisClient := initRedisClient(redisURL, consumerName)
	ctx := context.Background()
	// Start polling routine for messages
	go func() {
		log.Printf("consumer %s started polling for messages", redisClient.Config.ConsumerName)
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
