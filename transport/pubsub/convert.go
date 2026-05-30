package pubsub

import (
	"time"

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

	var notBefore time.Duration
	if nb, ok := attrs["X-NotBefore"]; ok {
		notBefore, _ = time.ParseDuration(nb)
	}

	return ringq.Message{
		ID:         msg.ID,
		Topic:      topic,
		Payload:    msg.Data,
		Attributes: attrs,
		NotBefore:  notBefore,
	}
}
