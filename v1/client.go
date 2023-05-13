package rediswrapper

import (
	"context"
	"fmt"
	"github.com/a-agmon/redis-streams-wrapper/generate"
	"github.com/redis/go-redis/v9"
	"log"
	"time"
)

type RedisClientConfig struct {
	Addr     string
	Username string
	Password string
	DB       int
}

type RedisStreamsClient struct {
	client       *redis.Client
	config       RedisClientConfig
	ConsumerName string
}

type RedisStreamsMessage struct {
	ID            string
	ConsumerName  string
	ConsumerGroup string
	StreamName    string
	Properties    map[string]interface{}
}

// NewRedisClientWrapper  creates a new RedisStreamsClient, it also accepts a RedisClientConfig struct as well as optional string for
// consumer name. If consumer name is not provided a random string will be generated.
// The idea is to create a stateless consumer/producer that can send/poll messages to/from any topic using any consumer group they choose
func NewRedisClientWrapper(config RedisClientConfig, consumerName string) *RedisStreamsClient {
	if consumerName == "" {
		consumerName = generate.RandomStringWithPrefix("consumer")
	}
	return &RedisStreamsClient{
		client: redis.NewClient(&redis.Options{
			Addr:     config.Addr,
			Username: config.Username,
			Password: config.Password,
			DB:       config.DB,
		}),
		config:       config,
		ConsumerName: consumerName,
	}
}

// CreateConsumerGroupIfNotExists creates a consumer group if it does not exist
// it requires the following parameters:
// streamKey: the stream key to create the consumer group on
// consumerGroup: the consumer group to create
func (r *RedisStreamsClient) CreateConsumerGroupIfNotExists(ctx context.Context, streamKey string, consumerGroup string) error {
	//validate that group name isnot empty
	if consumerGroup == "" {
		return fmt.Errorf("consumer group name cannot be empty")
	}
	err := r.client.XGroupCreateMkStream(ctx, streamKey, consumerGroup, "0").Err()
	if err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists" {
		log.Printf("Consumer group %s already exists on stream %s", consumerGroup, streamKey)
		return nil
	}
	if err != nil {
		return fmt.Errorf("error creating consumer group: %s on stream %s  - %v", consumerGroup, streamKey, err)
	}
	return nil
}

// ProduceMessage produces a message to the given stream key
// it requires the following parameters:
// streamKey: the stream key to produce the message to
// properties: a map of key value pairs that will be sent as part of the message
func (r *RedisStreamsClient) ProduceMessage(ctx context.Context, streamKey string, properties map[string]interface{}) error {
	id, err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: properties,
	}).Result()
	if err != nil {
		return fmt.Errorf("Error producing message: %v\n", err)
	}
	log.Printf("Produced message %s to stream: %s\n", id, streamKey)
	return nil
}

// FetchNewMessages polls for new messages for the given consumer group and returns RedisStreamsMessage
// it requires the following parameters:
// streamKey: the stream key to poll messages from
// consumerGroup: the consumer group to poll messages from
// count: the number of messages to poll
// waitForSeconds: how long to block for new messages. use 0 to block indefinitely
func (r *RedisStreamsClient) FetchNewMessages(ctx context.Context, streamKey string, consumerGroup string, count int, waitForSeconds int) ([]RedisStreamsMessage, error) {
	streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    consumerGroup,
		Consumer: r.ConsumerName,
		Streams:  []string{streamKey, ">"}, // ">" means read from the latest message
		Count:    int64(count),
		Block:    time.Duration(waitForSeconds) * time.Second,
	}).Result()
	if err != nil {
		if err == redis.Nil { // nothing was received after the block time
			return []RedisStreamsMessage{}, nil
		} else {
			return nil, fmt.Errorf("Error polling for new messages: %v\n", err)
		}
	}
	//there should only be one stream message (because we are only looking for messages from one stream)
	if len(streams) != 1 {
		err = fmt.Errorf("Consumer %s received %d new messages on group %s and should have recieved just one \n", r.ConsumerName, len(streams[0].Messages), consumerGroup)
		return nil, err
	}
	redisMessages := streams[0].Messages
	messages := make([]RedisStreamsMessage, 0, len(redisMessages))
	for _, redisMessage := range redisMessages {
		messages = append(messages, RedisStreamsMessage{
			ID:            redisMessage.ID,
			ConsumerName:  r.ConsumerName,
			ConsumerGroup: consumerGroup,
			StreamName:    streamKey,
			Properties:    redisMessage.Values,
		})
	}
	return messages, nil

}

// AckMessage acknowledges a message for the given consumer group and consumer name
// it requires the following parameters:
// streamKey: the stream key to acknowledge the message from
// consumerGroup: the consumer group to acknowledge the message from
// messageID: the message ID to acknowledge
func (r *RedisStreamsClient) AckMessage(ctx context.Context, streamKey string, consumerGroup string, messageID string) error {
	err := r.client.XAck(ctx, streamKey, consumerGroup, messageID).Err()
	if err != nil {
		return fmt.Errorf("Error acknowledging message: %v\n", err)
	}
	log.Printf("Consumer %s Acknowledged message %s on group %s \n", r.ConsumerName, messageID, consumerGroup)
	return nil
}

// ClaimMessagesNotAcked  claims pending messages for the given consumer group
// it requires the following parameters:
// streamKey: the stream key to claim messages from
// consumerGroup: the consumer group to claim messages from
// minIdleSeconds: the minimum idle time in seconds for a message to be considered for claiming - Return only messages that are idle for at least
func (r *RedisStreamsClient) ClaimMessagesNotAcked(ctx context.Context, streamKey string, consumerGroup string, count int64, minIdleSeconds int) ([]RedisStreamsMessage, error) {
	pending, err := r.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: streamKey,
		Group:  consumerGroup,
		Start:  "-", //earliest possible
		End:    "+", // latest possible
		Count:  count,
	}).Result()
	if err == redis.Nil {
		log.Printf("No pending messages found for group %s on stream %s by consumer %s \n", consumerGroup, streamKey, r.ConsumerName)
		return []RedisStreamsMessage{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("Error fetching pending messages: %v\n", err)
	}
	if len(pending) == 0 {
		log.Printf("No pending messages found for group %s on stream %s by consumer %s \n", consumerGroup, streamKey, r.ConsumerName)
		return []RedisStreamsMessage{}, nil
	}
	claimedMessages := make([]RedisStreamsMessage, 0)
	idleClaimedDuration := time.Duration(minIdleSeconds) * time.Second
	// Iterate over pending messages returned and claim them one by one
	for _, pendingMsg := range pending {
		log.Printf("Claiming pending message: %s from consumer %s \n", pendingMsg.ID, pendingMsg.Consumer)
		xclaimedMsgArray, err := r.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   streamKey,
			Group:    consumerGroup,
			Consumer: r.ConsumerName,
			MinIdle:  idleClaimedDuration,
			Messages: []string{pendingMsg.ID},
		}).Result()
		if err != nil {
			//TODO: need to decide how to handle this edge case - failure to claim a pending message
			fmt.Printf("Error claiming pending message %s: %v\n", pendingMsg.ID, err)
			continue
		}
		// this should be equal one since we are claiming one message at a time
		if len(xclaimedMsgArray) != 1 {
			return []RedisStreamsMessage{}, fmt.Errorf("expected 1 message to be claimed, got %d", len(xclaimedMsgArray))
		}
		xclaimedMsg := xclaimedMsgArray[0]
		log.Printf("Processing unclaimed message %s\n", xclaimedMsg.ID)
		claimedMessages = append(claimedMessages, r.transformXMessageToRedisStreamsMessage(&xclaimedMsg))
	}
	return claimedMessages, nil
}

func (r *RedisStreamsClient) ConsumerGroupExists(ctx context.Context, streamKey string, consumerGroup string) (bool, error) {
	groupsInfo, err := r.client.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		return false, err
	}
	for _, groupInfo := range groupsInfo {
		if groupInfo.Name == consumerGroup {
			return true, nil
		}
	}

	return false, nil
}

// TransformXMessageToRedisStreamsMessage Transform redis.XMessage to RedisStreamsMessage
func (r *RedisStreamsClient) transformXMessageToRedisStreamsMessage(xMessage *redis.XMessage) RedisStreamsMessage {
	return RedisStreamsMessage{
		ID:         xMessage.ID,
		Properties: xMessage.Values,
	}
}

// CloseConnection closeConnection closes the redis connection, though it should be alive and shared between routines.
func (r *RedisStreamsClient) CloseConnection() {
	if r.client != nil {
		err := r.client.Close()
		if err != nil {
			log.Printf("Error closing redis connection: %v\n", err)
			panic(err)
		}
	}

}
