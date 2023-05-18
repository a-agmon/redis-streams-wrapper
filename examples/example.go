package main

import (
	"context"
	"log"
	rediswrapper "github.com/a-agmon/redis-streams-wrapper/v1"
)

func main() {

	exampleStreamName := "books-order-stream"
	exampleGroupName := "books-order-group"
	// create a new client wrapper - consumer name is optional - will be created if not provided
	client := rediswrapper.NewRedisClientWrapper(rediswrapper.RedisClientConfig{Addr: "localhost:6379"})
	log.Printf("Client created with consumer name %s", client.Config.ConsumerName)

	// produce a message
	ctx := context.Background()
	err := client.ProduceMessage(ctx, exampleStreamName, map[string]interface{}{
		"book":   "The Sun Also Rises",
		"author": "Earnest Hemingway",
	})
	if err != nil {
		panic(err)
	}
	log.Printf("Waiting 5 seconds for 3 messages to arrive")
	err = client.FetchNewMessagesWithCB(
		ctx, exampleStreamName, exampleGroupName, 3, 5,
		func(msgID string, payload map[string]interface{}) {
			log.Printf("NewMessage!\n\tConsumer:%s\n\tmessage:%s\n\tPayload:%v\n\n",
				client.Config.ConsumerName,
				msgID,
				payload)
			err = client.AckMessage(ctx, exampleStreamName, exampleGroupName, msgID)
			if err != nil {
				panic(err)
			}
		})
	if err != nil {
		panic(err)
	}

}
