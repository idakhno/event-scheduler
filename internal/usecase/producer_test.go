package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"event_scheduler/internal/domain"
)

type mockMessageQueue struct {
	publishFunc func(ctx context.Context, topic string, message []byte) error
}

func (m *mockMessageQueue) Publish(ctx context.Context, topic string, message []byte) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, topic, message)
	}
	return nil
}

func (m *mockMessageQueue) Receive(ctx context.Context, topic string) ([]byte, error) {
	return nil, errors.New("not implemented")
}

func (m *mockMessageQueue) Ack(ctx context.Context) error {
	return errors.New("not implemented")
}

func (m *mockMessageQueue) Close() error {
	return nil
}

func TestProducer_produceEvent(t *testing.T) {
	t.Run("successful event production", func(t *testing.T) {
		var publishedTopic string
		var publishedData []byte

		mockQueue := &mockMessageQueue{
			publishFunc: func(ctx context.Context, topic string, message []byte) error {
				publishedTopic = topic
				publishedData = message
				return nil
			},
		}

		logger := slog.Default()
		producer := NewProducer(
			mockQueue,
			"test-topic",
			[]string{"event1", "event2"},
			100,
			5,
			10,
			logger,
		)

		ctx := context.Background()
		err := producer.produceEvent(ctx)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if publishedTopic != "test-topic" {
			t.Errorf("expected topic %q, got %q", "test-topic", publishedTopic)
		}

		if len(publishedData) == 0 {
			t.Fatal("published data should not be empty")
		}

		var event domain.Event
		if err := json.Unmarshal(publishedData, &event); err != nil {
			t.Fatalf("failed to unmarshal published event: %v", err)
		}

		if event.IdempotencyKey == "" {
			t.Error("event IdempotencyKey should not be empty")
		}

		if event.Type == "" {
			t.Error("event Type should not be empty")
		}

		if event.Status != domain.StatusScheduled {
			t.Errorf("expected Status %q, got %q", domain.StatusScheduled, event.Status)
		}

		if event.NextRunAt == nil {
			t.Fatal("event NextRunAt should not be nil")
		}

		now := time.Now()
		if event.NextRunAt.Before(now) {
			t.Error("event NextRunAt should be in the future")
		}
	})

	t.Run("publish error", func(t *testing.T) {
		publishErr := errors.New("publish failed")
		mockQueue := &mockMessageQueue{
			publishFunc: func(ctx context.Context, topic string, message []byte) error {
				return publishErr
			},
		}

		logger := slog.Default()
		producer := NewProducer(
			mockQueue,
			"test-topic",
			[]string{"event1"},
			100,
			5,
			10,
			logger,
		)

		ctx := context.Background()
		err := producer.produceEvent(ctx)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, publishErr) && err.Error() != "publish event: "+publishErr.Error() {
			t.Errorf("expected publish error, got %v", err)
		}
	})
}

func TestProducer_NewProducer(t *testing.T) {
	mockQueue := &mockMessageQueue{}
	logger := slog.Default()

	producer := NewProducer(
		mockQueue,
		"test-topic",
		[]string{"event1", "event2"},
		100,
		5,
		10,
		logger,
	)

	if producer == nil {
		t.Fatal("NewProducer returned nil")
	}

	if producer.topic != "test-topic" {
		t.Errorf("expected topic %q, got %q", "test-topic", producer.topic)
	}

	if len(producer.eventTypes) != 2 {
		t.Errorf("expected 2 event types, got %d", len(producer.eventTypes))
	}

	if producer.payloadMaxValue != 100 {
		t.Errorf("expected payloadMaxValue 100, got %d", producer.payloadMaxValue)
	}

	if producer.delayMinSeconds != 5 {
		t.Errorf("expected delayMinSeconds 5, got %d", producer.delayMinSeconds)
	}

	if producer.delayMaxSeconds != 10 {
		t.Errorf("expected delayMaxSeconds 10, got %d", producer.delayMaxSeconds)
	}
}
