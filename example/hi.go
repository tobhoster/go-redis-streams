package main

import (
	streams "github.com/tobhoster/go-redis-streams"
)

func main() {
	payload := "{\"name\":\"John\", \"age\":30, \"car\":null}"

	key := streams.Key("testing")

	redis := streams.Redis{}
	redis.Init()

	// Publish
	redis.Publish(key, payload)

	groupId := "FOLLOW_TESTING"

	data := make(chan streams.PayloadMessage)
	go redis.Subscribe(key, groupId, "TESTING", data)

	for {
		message := <-data

		redis.AcknowledgeMessage(message.Channel, message.GroupID, message.ID)

		if payload == message.Raw["payload"] {
			break
		}
	}
}
