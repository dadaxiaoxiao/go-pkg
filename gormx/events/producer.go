package events

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
)

//go:generate mockgen.exe -source=./producer.go -package=evtmocks -destination=mocks/producer.mock.go Producer
type Producer interface {
	ProduceInconsistentEvent(ctx context.Context, evt InconsistentEvent) error
}

type SaramaProducer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewSaramaProducer 新建sarama producer
func NewSaramaProducer(producer sarama.SyncProducer, topic string) *SaramaProducer {
	return &SaramaProducer{
		producer: producer,
		topic:    topic,
	}
}

func (s *SaramaProducer) ProduceInconsistentEvent(ctx context.Context, evt InconsistentEvent) error {
	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	_, _, err = s.producer.SendMessage(&sarama.ProducerMessage{
		Topic: s.topic,
		Value: sarama.ByteEncoder(data),
	})
	return err
}
