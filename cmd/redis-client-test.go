package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"math/rand"
	"time"
)

const (
	StreamKey       = "newstream"
	ConsumerGroup   = "mygroup"
	PendingClaimDur = 5 * time.Second
)

var ConsumerName = randomString()

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	ctx := context.Background()
	log.Printf("Starting consumer [%s] part of group [%s] on stream [%s]\n", ConsumerName, ConsumerGroup, StreamKey)
	//create a consumer group
	// If we provide $ in start, then only new messages arriving in the stream from now on will be provided to the consumers in the group.
	// If we specify 0 instead the consumer group will consume all the messages in the stream history to start with.
	err := rdb.XGroupCreate(ctx, StreamKey, ConsumerGroup, "0").Err()
	if err != nil && err != redis.Nil {
		fmt.Printf("Error creating consumer group: %v\n", err)
	}
	log.Print("Consumer group created or already exists")
	claimPendingMessages(ctx, rdb)
	consumePendingMessages(ctx, rdb)
	pollForNewMessages(ctx, rdb)
}

// implement the processMessage function below to just print all the message details
func processMessage(id string, values map[string]interface{}) {
	fmt.Printf("Processing message: %s\n", id)
	for k, v := range values {
		fmt.Printf("%s: %s\n", k, v)
	}
}

func claimPendingMessages(ctx context.Context, rdb *redis.Client) {
	log.Print("Claiming pending messages not acked by previous consumer")
	for {
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: StreamKey,
			Group:  ConsumerGroup,
			Start:  "-", //earliest possible
			End:    "+", // latest possible
			Count:  10,
		}).Result()
		if err != nil {
			fmt.Printf("Error fetching pending messages: %v\n", err)
			return
		}
		if len(pending) == 0 {
			log.Print("No pending messages found for group")
			break
		}

		for _, msg := range pending {
			log.Printf("Claiming pending message: %s from consumer %s \n", msg.ID, msg.Consumer)
			claimedMsg, err := rdb.XClaim(ctx, &redis.XClaimArgs{
				Stream:   StreamKey,
				Group:    ConsumerGroup,
				Consumer: ConsumerName,
				MinIdle:  PendingClaimDur,
				Messages: []string{msg.ID},
			}).Result()
			if err != nil {
				fmt.Printf("Error claiming pending message: %v\n", err)
				return
			}
			// check if we got messages to claim and if so then log them
			if len(claimedMsg) > 0 {
				for _, message := range claimedMsg {
					log.Print("Processing unclaimed message")
					processMessage(message.ID, message.Values)
					rdb.XAck(ctx, StreamKey, ConsumerGroup, message.ID)
				}
			}

		}
	}
}

func consumePendingMessages(ctx context.Context, rdb *redis.Client) {
	log.Print("Consuming pending messages not acked by THIS consumer")
	for {
		result, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroup,
			Consumer: ConsumerName,
			Streams:  []string{StreamKey, "0"},
			Count:    10,
			Block:    0,
		}).Result()
		if err != nil {
			fmt.Printf("Error reading pending messages: %v\n", err)
			return
		}
		// the result can return multiple streams
		if len(result[0].Messages) == 0 {
			log.Print("No pending messages found for consumer")
			break
		}

		for _, stream := range result {
			for _, message := range stream.Messages {
				processMessage(message.ID, message.Values)
				rdb.XAck(ctx, StreamKey, ConsumerGroup, message.ID)
			}
		}
	}
}
func pollForNewMessages(ctx context.Context, rdb *redis.Client) {
	log.Print("Polling for new messages")
	for {
		result, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroup,
			Consumer: ConsumerName,
			Streams:  []string{StreamKey, ">"},
			Count:    10,
			Block:    0,
		}).Result()
		if err != nil {
			fmt.Printf("Error polling new messages: %v\n", err)
			return
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				processMessage(message.ID, message.Values)
				rdb.XAck(ctx, StreamKey, ConsumerGroup, message.ID)
			}
		}
	}
}

// Generate a random string in the format AAA-111
func randomString() string {
	// Set up the source of letters and numbers to use
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Initialize a new random seed
	rand.Seed(time.Now().UnixNano())

	// Generate 3 random letters
	letters := make([]byte, 3)
	for i := range letters {
		letters[i] = charset[rand.Intn(len(charset)-10)] // exclude digits from charset
	}

	// Generate 3 random digits
	digits := make([]byte, 3)
	for i := range digits {
		digits[i] = charset[rand.Intn(10)] // only choose digits from charset
	}

	// Concatenate the letters and digits into the final string
	return fmt.Sprintf("%s-%s", string(letters), string(digits))
}
