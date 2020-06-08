package kfktool

import (
	"github.com/Shopify/sarama"
)

// Publish ...
func Publish(topic string, message string) (p int32, off int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	p, off, err = producer.SendMessage(msg)
	return
}
