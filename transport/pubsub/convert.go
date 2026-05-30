package pubsub

import (
	"cloud.google.com/go/pubsub"
	"github.com/vianhanif/go-task-orbit/ringq"
)

func toRingqMessage(msg *pubsub.Message, topicKey string) ringq.Message {
	topic := ""
	if topicKey != "" && msg.Attributes != nil {
		topic = msg.Attributes[topicKey]
	}

	attrs := make(map[string]string, len(msg.Attributes))
	for k, v := range msg.Attributes {
		attrs[k] = v
	}

	return ringq.Message{
		ID:         msg.ID,
		Topic:      topic,
		Payload:    msg.Data,
		Attributes: attrs,
	}
}
