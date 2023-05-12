package main

import (
	"context"
	"github.com/a-agmon/redis-streams-wrapper/lib"
	"log"
)

func main() {

	// create a new client wrapper
	client := lib.NewRedisClient(lib.RedisClientConfig{
		Addr: "localhost:6379",
	}, ",my test consumer")

	// produce a message
	ctx := context.Background()
	err := client.ProduceMessage(ctx, "books-order-stream", map[string]interface{}{
		"book":   "The Sun Also Rises",
		"author": "Earnest Hemingway",
	})
	if err != nil {
		panic(err)
	}

	// wait 5 seconds for 3 messages to arrive
	messages, err := client.FetchNewMessages(
		ctx,
		"books-order-stream",
		"books-order-group",
		1,
		5)
	if err != nil {
		panic(err)
	}
	// print and ack every message
	for _, message := range messages {
		log.Printf("NewMessage!\n\tConsumer:%s\n\tmessage:%s\n\tPayload:%v\n\n",
			client.ConsumerName,
			message.ID,
			message.Properties)
		err := client.AckMessage(ctx, "books-order-stream", "books-order-group", message.ID)
		if err != nil {
			panic(err)
		}
	}

}
