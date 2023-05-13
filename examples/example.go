package main

import (
	"context"
	"github.com/a-agmon/redis-streams-wrapper/v1"
	"log"
)

func main() {

	exampleStreamName := "books-order-stream"
	exampleGroupName := "books-order-group"
	// create a new client wrapper
	client := rediswrapper.NewRedisClientWrapper(rediswrapper.RedisClientConfig{
		Addr: "localhost:6379",
	}, "")
	log.Printf("Client created with consumer name %s", client.ConsumerName)

	// produce a message
	ctx := context.Background()
	err := client.ProduceMessage(ctx, exampleStreamName, map[string]interface{}{
		"book":   "The Sun Also Rises",
		"author": "Earnest Hemingway",
	})
	if err != nil {
		panic(err)
	}
	// create a consumer group if it does not exist
	err = client.CreateConsumerGroupIfNotExists(context.Background(), exampleStreamName, exampleGroupName)
	if err != nil {
		panic(err)
	}
	// wait 5 seconds for 3 messages to arrive
	log.Printf("Waiting 5 seconds for 3 messages to arrive")
	messages, err := client.FetchNewMessages(
		ctx,
		exampleStreamName,
		exampleGroupName,
		3,
		5)
	log.Printf("Messages arrived!")
	if err != nil {
		panic(err)
	}
	// print and ack every message
	for _, message := range messages {
		log.Printf("NewMessage!\n\tConsumer:%s\n\tmessage:%s\n\tPayload:%v\n\n",
			client.ConsumerName,
			message.ID,
			message.Properties)
		err := client.AckMessage(ctx, exampleStreamName, exampleGroupName, message.ID)
		if err != nil {
			panic(err)
		}
	}

}
