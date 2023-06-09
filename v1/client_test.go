package rediswrapper

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/a-agmon/redis-streams-wrapper/generate"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

var testStreamName = generate.RandomStringWithPrefix("STREAM")
var testConsumerGroup = generate.RandomStringWithPrefix("GROUP")
var client *RedisStreamsClient

func TestMain(m *testing.M) {
	log.Print("Starting mini redis server")
	s, err := miniredis.Run()
	if err != nil {
		log.Fatalf("Error creating miniredis server: %v", err)
	}
	log.Printf("Miniredis server created on %v", s.Addr())
	client = NewRedisClientWrapper(RedisClientConfig{
		Addr:     s.Addr(),
		DB:       0,
		Password: "",
		Username: "",
	})

	m.Run()
	s.Close()
}

// todo: we need to test for bad consumer names

func TestProduceMessage(t *testing.T) {
	//produce a message
	err := client.ProduceMessage(context.Background(), testStreamName, map[string]interface{}{
		"testProperty": "testValue",
	})
	if err != nil {
		t.Fatalf("Error producing message: %v", err)
	}
	//create a consumer group
	err = client.createConsumerGroupIfNotExists(context.Background(), testStreamName, testConsumerGroup)
	if err != nil {
		t.Fatalf("Error creating consumer group: %v", err)
	}
	//check that the message was produced using the FetchNewMessages function
	messages, err := client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 1, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %v", len(messages))
	}
	assert.EqualValues(t, messages[0].Properties["testProperty"], "testValue")
	//Ack the message to make sure everything is clean
	err = client.AckMessage(context.Background(), testStreamName, testConsumerGroup, messages[0].ID)
	if err != nil {
		t.Fatalf("Error acking message: %v", err)
	}
}
func produceMessages(count int, t *testing.T, client *RedisStreamsClient) {
	for i := 0; i < count; i++ {
		err := client.ProduceMessage(context.Background(), testStreamName, map[string]interface{}{
			"messageindex": i,
		})
		if err != nil {
			t.Fatalf("Error acking message: %v", err)
		}
	}
}

func TestPollAndAck(t *testing.T) {
	//generate 10 messages
	produceMessages(10, t, client)
	//check that the message was produced - we poll 5/10
	messages, err := client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 5, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, len(messages), 5)
	// now we should get 5 messages because 5/10 were already pulled though not acked
	messages, err = client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 100, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, len(messages), 5)
	// now we should get 0 messages because they were all pulled
	messages, err = client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 100, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, len(messages), 0)
	// now lets clain thewm
	claimedMessages, err := client.ClaimMessagesNotAcked(context.Background(), testStreamName, testConsumerGroup, 10, 1)
	if err != nil {
		t.Fatalf("Error claiming messages: %v", err)
	}
	assert.EqualValues(t, len(claimedMessages), 10)
	// we did not ack so claining again should return 10 as well - but we will sleep for 4 seconds to make sure
	time.Sleep(4 * time.Second)
	claimedMessages, err = client.ClaimMessagesNotAcked(context.Background(), testStreamName, testConsumerGroup, 10, 1)
	if err != nil {
		t.Fatalf("Error claiming messages: %v", err)
	}
	assert.EqualValues(t, len(claimedMessages), 10)
	// now ack the messages
	for _, message := range claimedMessages {
		err = client.AckMessage(context.Background(), testStreamName, testConsumerGroup, message.ID)
		if err != nil {
			t.Fatalf("Error acking message: %v", err)
		}
	}
	//now nothing should be claimed
	claimedMessages, err = client.ClaimMessagesNotAcked(context.Background(), testStreamName, testConsumerGroup, 10, 1)
	if err != nil {
		t.Fatalf("Error claiming messages: %v", err)
	}
	assert.EqualValues(t, len(claimedMessages), 0)
	//now nothing should be polled
	messages, err = client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 100, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, len(messages), 0)
}

func TestPollAndAckWithCB(t *testing.T) {
	produceMessages(10, t, client)
	var messagesProcessed int
	err := client.FetchNewMessagesWithCB(context.Background(),
		testStreamName,
		testConsumerGroup,
		20, 3,
		func(id string, props map[string]interface{}) {
			//ack the message
			err := client.AckMessage(context.Background(), testStreamName, testConsumerGroup, id)
			if err != nil {
				t.Fatalf("Error acking message: %v", err)
			}
			messagesProcessed++
		})
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, messagesProcessed, 10)
}

func TestContinousPol(t *testing.T) {
	produceMessages(100, t, client)
	// loop though them in chunks of 10
	for i := 0; i < 10; i++ {
		messages, err := client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 10, 3)
		if err != nil {
			t.Fatalf("Error polling for new messages: %v", err)
		}
		assert.EqualValues(t, len(messages), 10)
		for _, message := range messages {
			err = client.AckMessage(context.Background(), testStreamName, testConsumerGroup, message.ID)
			if err != nil {
				t.Fatalf("Error acking message: %v", err)
			}
		}
	}
	//now nothing should be polled
	messages, err := client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 10, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, 0, len(messages))

}

func TestCreatingGroup(t *testing.T) {
	//create a consumer group
	err := client.createConsumerGroupIfNotExists(context.Background(), testStreamName, "GROUP1")
	if err != nil {
		t.Fatalf("Error creating consumer group: %v", err)
	}
	//create a consumer group with the same name
	err = client.createConsumerGroupIfNotExists(context.Background(), testStreamName, "GROUP1")
	if err != nil {
		t.Fatalf("Error creating consumer group: %v", err)
	}
	err = client.createConsumerGroupIfNotExists(context.Background(), testStreamName, "")
	if err == nil {
		t.Fail()
	}
}

func TestBlockedRead(t *testing.T) {
	produceMessages(5, t, client)
	// we poll for 10 messages but only 5 are there
	messages, err := client.FetchNewMessages(context.Background(), testStreamName, testConsumerGroup, 10, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, len(messages), 5)
}

func TestConsumerGroupExists(t *testing.T) {
	knownGroup := generate.RandomStringWithPrefix("NEWGROUP1")
	unknownGroup := generate.RandomStringWithPrefix("NEWGROUP2")

	// create a stream
	groupTestStreamName := generate.RandomStringWithPrefix("GROUPTESTSTREAM")
	err := client.ProduceMessage(context.Background(), groupTestStreamName, map[string]interface{}{"test": "test"})
	if err != nil {
		t.Fatalf("Error creating consumer group: %v", err)
	}
	//create a consumer group
	err = client.createConsumerGroupIfNotExists(context.Background(), groupTestStreamName, knownGroup)
	if err != nil {
		t.Fatalf("Error creating consumer group: %v", err)
	}
	//check if it exists
	exists, err := client.ConsumerGroupExists(context.Background(), groupTestStreamName, knownGroup)
	if err != nil {
		t.Fatalf("Error checking consumer group: %v", err)
	}
	assert.True(t, exists)
	//check if the new one exists
	exists, err = client.ConsumerGroupExists(context.Background(), groupTestStreamName, unknownGroup)
	if err != nil {
		t.Fatalf("Error checking consumer group: %v", err)
	}
	assert.False(t, exists)
	// start polling with the new group and make sure it is added automatically
	messages, err := client.FetchNewMessages(context.Background(), groupTestStreamName, unknownGroup, 5, 3)
	if err != nil {
		t.Fatalf("Error polling for new messages: %v", err)
	}
	assert.EqualValues(t, 1, len(messages))

}

// test closeConnection  must always run last
func TestCloseConnection(t *testing.T) {
	client.CloseConnection()

}
