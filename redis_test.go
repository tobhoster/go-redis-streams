package go_redis_streams

import (
	"testing"
)

func TestRedis_Publish(t *testing.T) {

	payload := "{\"name\":\"John\", \"age\":30, \"car\":null}"

	key := Key("testing")
	expectedKey := "streams:testing"

	if key != expectedKey {
		t.Errorf("got %q, wanted %q", key, expectedKey)
	}

	redis := Redis{}
	redis.Init()

	// Publish
	redis.Publish(key, payload)

	groupId := "FOLLOW_TESTING"

	data := make(chan PayloadMessage)
	go redis.Subscribe(key, groupId, "TESTING", data)

	for {
		message := <-data

		redis.AcknowledgeMessage(message.Channel, message.GroupID, message.ID)

		if payload == message.Raw["payload"] {
			break
		} else {
			t.Failed()
		}
	}
}
