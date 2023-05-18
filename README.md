## redis-streams-wrapper
### Opinionated Kafka-like API for Redis Streams

Redis Streams is a performant stream processing and messaging solution that is based on Redis.
For many use cases, it's a much simpler and cheaper alternative to Kafka.
Some of its advantages over Kafka are that its not based on partitions, its parallelism is not limited by partition number and therefore consumer applications can be easliy scaled,
and it is relatively lean and simple to maintain. 
On the other hand, it is less durable and, as is the case with Redis, uses in process memory for persistance (i.e., rather than being an append log server like Kafka it stores everything in memory).
One of its disadvantages, however, is its syntax, that can be cumbersome to understand and not easy to use. 
This project makes use of the wonderful redis-go library and wraps Redis-Streams with a Kafka-like API

Using it is fairly simple:

```go

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
	client := rediswrapper.NewRedisClientWrapper(
		rediswrapper.RedisClientConfig{Addr: "localhost:6379"}
	)
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
```
