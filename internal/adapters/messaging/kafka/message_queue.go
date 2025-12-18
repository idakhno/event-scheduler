// Package kafka provides Kafka implementation of the message queue interface.
package kafka

import (
	"context"
	"fmt"
	"time"

	"event_scheduler/internal/ports"

	"github.com/segmentio/kafka-go"
)

type MessageQueue struct {
	writer         *kafka.Writer
	reader         *kafka.Reader
	brokers        []string
	consumerGroup  string
	lastMessage    *kafka.Message
	minBytes       int
	maxBytes       int
	maxWait        time.Duration
	commitInterval time.Duration
}

func NewMessageQueue(
	brokers []string,
	consumerGroup string,
	batchSize int,
	batchBytes int64,
	batchTimeout time.Duration,
	minBytes int,
	maxBytes int,
	maxWait time.Duration,
	requiredAcks int,
	commitInterval time.Duration,
) *MessageQueue {
	var acks kafka.RequiredAcks
	switch requiredAcks {
	case -1:
		acks = kafka.RequireAll
	case 0:
		acks = kafka.RequireNone
	default:
		acks = kafka.RequireOne
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    batchSize,
		BatchBytes:   batchBytes,
		BatchTimeout: batchTimeout,
		RequiredAcks: acks,
		Async:        false,
	}

	return &MessageQueue{
		writer:         writer,
		brokers:        brokers,
		consumerGroup:  consumerGroup,
		minBytes:       minBytes,
		maxBytes:       maxBytes,
		maxWait:        maxWait,
		commitInterval: commitInterval,
	}
}

func (q *MessageQueue) Publish(ctx context.Context, topic string, message []byte) error {
	return q.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Value: message,
	})
}

func (q *MessageQueue) Receive(ctx context.Context, topic string) ([]byte, error) {
	if q.reader == nil {
		q.reader = kafka.NewReader(kafka.ReaderConfig{
			Brokers:        q.brokers,
			Topic:          topic,
			GroupID:        q.consumerGroup,
			MinBytes:       q.minBytes,
			MaxBytes:       q.maxBytes,
			MaxWait:        q.maxWait,
			CommitInterval: q.commitInterval,
		})
	}

	msg, err := q.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	q.lastMessage = &msg
	return msg.Value, nil
}

func (q *MessageQueue) Ack(ctx context.Context) error {
	if q.reader == nil || q.lastMessage == nil {
		return nil
	}
	err := q.reader.CommitMessages(ctx, *q.lastMessage)
	q.lastMessage = nil
	return err
}

func (q *MessageQueue) Close() error {
	var errs []error

	if q.writer != nil {
		if err := q.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if q.reader != nil {
		if err := q.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing kafka: %v", errs)
	}

	return nil
}

var _ ports.MessageQueue = (*MessageQueue)(nil)
