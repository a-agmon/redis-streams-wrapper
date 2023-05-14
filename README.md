## redis-streams-wrapper
### Kafka-like API for Redis Streams

Redis Streams is a performant stream processing and messaging solution that is based on Redis.
For many use cases it is a simpler and cheaper alternative to Kafka. Some of its advantages over Kafka is that its not based on partitions, its parallelism is not limited by partition number and therefore consumer applications can be easliy scaled, and it is relatively lean and simple to maintain. On the other hand, it is less durable and does not persist data (rather than being an append log server like Kafka it stores everything in memory).
One of its disadvantages, however, is its syntax, that can be cumbersome to understand and not easy to use. 
This project makes use of redis-go library and wraps it with a Kafka-like API

Using it is fairly simple

```go

import "github.com/a-agmon/redis-streams-wrapper/v1"
    // create a client
client := rediswrapper.NewRedisClientWrapper(rediswrapper.RedisClientConfig{
    Addr: "localhost:6379",
}, "")

// produce a message
ctx := context.Background()
err := client.ProduceMessage(ctx, exampleStreamName, map[string]interface{}{
    "book":   "The Sun Also Rises",
    "author": "Earnest Hemingway",
})
// create a consumer group if it does not exist
err = client.CreateConsumerGroupIfNotExists(context.Background(), 
	exampleStreamName, exampleGroupName)
// wait 5 seconds for 3 messages to arrive
err = client.FetchNewMessagesWithCB(
    ctx, exampleStreamName, exampleGroupName, 3, 5,
    func(msgID string, payload map[string]interface{}) {
        log.Printf("NewMessage!\n\tConsumer:%s\n\tmessage:%s\n\tPayload:%v\n\n",
            client.ConsumerName,
            msgID,
            payload)
        //Ack the message
        err = client.AckMessage(ctx, exampleStreamName, exampleGroupName, msgID)
    })
```

Redis can manage offeset tracking using consumer groups, and can reclaim any messages not acked by other consumers:

```go
claimedMessages, err = client.ClaimMessagesNotAcked(context.Background(), 
	testStreamName, testConsumerGroup, 10, 1)
	if err != nil {
	...
	}
	// now ack the messages
	for _, message := range claimedMessages {
		err = client.AckMessage(context.Background(), testStreamName, 
			testConsumerGroup, message.ID)
		if err != nil {
			...
		}
	}
```
