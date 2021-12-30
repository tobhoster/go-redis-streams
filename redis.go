package go_redis_streams

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
	"log"
	"os"
)

var ctx = context.Background()

type Redis struct {
	client *redis.Client
}

type PayloadMessage struct {
	Channel string                 `json:"channel"`
	GroupID string                 `json:"groupID"`
	ID      string                 `json:"ID"`
	Message string                 `json:"message"`
	Raw     map[string]interface{} `json:"raw"`
}

type Streams interface {
	Init() Redis
	Key()
	Publish()
	Subscribe()
	AcknowledgeMessage()
}

// Init Initialize Redis
func (r *Redis) Init() *Redis {
	err := godotenv.Load()
	if err != nil {
		log.Println("Error loading .env file")
	}

	host := os.Getenv("REDIS_HOST")
	port := os.Getenv("REDIS_PORT")
	password := os.Getenv("REDIS_PASSWORD")

	if len(host) == 0 || len(port) == 0 || len(password) == 0 {
		log.Fatal("🚨 No Env variable provided - REDIS_HOST, REDIS_PORT & REDIS_PASSWORD")
	}

	address := fmt.Sprintf("%s:%s", host, port)
	r.client = redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password, // no password set
		DB:       0,        // use default DB
	})

	result, err := r.client.Ping(ctx).Result()
	if err != nil {
		return &Redis{}
	}
	log.Println("PING: ", result)

	return r
}

// Key Create Key
func Key(key string) string {
	return fmt.Sprintf("streams:%s", key)
}

// Publish - Publish Redis Stream Message
func (r *Redis) Publish(streamKey string, payload string) {
	// Add Stream Key
	r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 10000,
		ID:     "*",
		Values: map[string]interface{}{
			"payload": payload,
		},
	})
}

// Subscribe - Subscribe Stream using Stream Key, GroupId and Consumer Name
//
// Example: go redis.Subscribe(key, groupId, "TESTING", data)
func (r *Redis) Subscribe(streamKey, groupId, consumerId string, data chan PayloadMessage) {
	_, err := r.client.XGroupCreate(ctx, streamKey, groupId, "$").Result()
	if err != nil {
		log.Println("An Error Occurred | XGroupCreate: ", err.Error())
	}

	lastId := "0-0"
	checkBacklog := true

	log.Printf("🏁 Consumer %s Starting...\n", consumerId)
	for {
		myId := lastId
		if !checkBacklog {
			myId = ">"
		}

		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupId,
			Consumer: consumerId,
			Streams:  []string{streamKey, myId},
			NoAck:    false,
		}).Result()

		if err != nil {
			log.Println("🚨 Subscribe | An Error Occurred | XReadGroup: ", err.Error())
		}

		for _, stream := range streams {
			streamKey := stream.Stream

			// Stream Message
			for _, message := range stream.Messages {
				messageId := message.ID

				// Struct Payload Message
				payload := PayloadMessage{
					Channel: streamKey,
					GroupID: groupId,
					ID:      messageId,
					Message: "",
					Raw:     message.Values,
				}

				// Send Message to Channel to be read
				data <- payload
			}

			if len(stream.Messages) == 0 && checkBacklog {
				checkBacklog = false
			}
		}
	}
}

// AcknowledgeMessage Acknowledge Redis Stream Message
func (r *Redis) AcknowledgeMessage(streamKey, groupId, messageId string) {
	_, err := r.client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.XAck(ctx, streamKey, groupId, messageId)
		pipeliner.XTrimMaxLen(ctx, streamKey, 1000)
		pipeliner.XDel(ctx, streamKey, messageId)
		_, err := pipeliner.Exec(ctx)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Println("🚨 AcknowledgeMessage | An Error Occurred | TxPipelined: ", err.Error())
	}
}
