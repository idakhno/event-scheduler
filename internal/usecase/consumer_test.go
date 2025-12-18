package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"event_scheduler/internal/domain"

	"github.com/google/uuid"
)

type mockEventRepository struct {
	createFunc func(ctx context.Context, event *domain.Event) (bool, error)
	getFunc    func(ctx context.Context, key string) (*domain.Event, error)
	updateFunc func(ctx context.Context, event *domain.Event) error
}

func (m *mockEventRepository) Create(ctx context.Context, event *domain.Event) (bool, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, event)
	}
	return true, nil
}

func (m *mockEventRepository) Get(ctx context.Context, key string) (*domain.Event, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, key)
	}
	return nil, nil
}

func (m *mockEventRepository) Update(ctx context.Context, event *domain.Event) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, event)
	}
	return nil
}

type mockMessageQueueForConsumer struct {
	publishFunc func(ctx context.Context, topic string, message []byte) error
	receiveFunc func(ctx context.Context, topic string) ([]byte, error)
	ackFunc     func(ctx context.Context) error
}

func (m *mockMessageQueueForConsumer) Publish(ctx context.Context, topic string, message []byte) error {
	if m.publishFunc != nil {
		return m.publishFunc(ctx, topic, message)
	}
	return nil
}

func (m *mockMessageQueueForConsumer) Receive(ctx context.Context, topic string) ([]byte, error) {
	if m.receiveFunc != nil {
		return m.receiveFunc(ctx, topic)
	}
	return nil, errors.New("not implemented")
}

func (m *mockMessageQueueForConsumer) Ack(ctx context.Context) error {
	if m.ackFunc != nil {
		return m.ackFunc(ctx)
	}
	return nil
}

func (m *mockMessageQueueForConsumer) Close() error {
	return nil
}

func TestConsumer_executeTask(t *testing.T) {
	t.Run("successful execution", func(t *testing.T) {
		logger := slog.Default()
		consumer := &Consumer{
			taskErrorChance:       0, // no errors
			taskExecutionDuration: 10 * time.Millisecond,
			logger:                logger,
		}

		event := &domain.Event{
			ID:   uuid.Must(uuid.NewV7()),
			Type: "test_event",
		}

		start := time.Now()
		err := consumer.executeTask(event)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if duration < 10*time.Millisecond {
			t.Errorf("expected execution to take at least 10ms, got %v", duration)
		}
	})

	t.Run("random failure", func(t *testing.T) {
		logger := slog.Default()
		consumer := &Consumer{
			taskErrorChance:       1, // 100% chance of error
			taskExecutionDuration: 1 * time.Millisecond,
			logger:                logger,
		}

		event := &domain.Event{
			ID:   uuid.Must(uuid.NewV7()),
			Type: "test_event",
		}

		err := consumer.executeTask(event)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if err.Error() != "random failure" {
			t.Errorf("expected error 'random failure', got %q", err.Error())
		}
	})
}

func TestConsumer_consumeOne(t *testing.T) {
	t.Run("unmarshal error - sends to DLQ", func(t *testing.T) {
		invalidData := []byte("invalid json")

		var dlqPublished bool
		var acked bool

		mockQueue := &mockMessageQueueForConsumer{
			receiveFunc: func(ctx context.Context, topic string) ([]byte, error) {
				return invalidData, nil
			},
			publishFunc: func(ctx context.Context, topic string, message []byte) error {
				if topic == "test-dlq" {
					dlqPublished = true
				}
				return nil
			},
			ackFunc: func(ctx context.Context) error {
				acked = true
				return nil
			},
		}

		mockRepo := &mockEventRepository{}

		logger := slog.Default()
		consumer := &Consumer{
			queue:    mockQueue,
			topic:    "test-topic",
			dlqTopic: "test-dlq",
			repo:     mockRepo,
			logger:   logger,
		}

		ctx := context.Background()
		err := consumer.consumeOne(ctx)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !dlqPublished {
			t.Error("corrupted message should have been sent to DLQ")
		}

		if !acked {
			t.Error("message should have been acked after DLQ publish")
		}
	})

	t.Run("duplicate event - ack without creating", func(t *testing.T) {
		event := domain.NewEvent("test-key", "test_type", map[string]any{"key": "value"}, time.Now().Add(5*time.Minute))
		eventData, _ := json.Marshal(event)

		var acked bool
		var created bool

		mockQueue := &mockMessageQueueForConsumer{
			receiveFunc: func(ctx context.Context, topic string) ([]byte, error) {
				return eventData, nil
			},
			ackFunc: func(ctx context.Context) error {
				acked = true
				return nil
			},
		}

		mockRepo := &mockEventRepository{
			createFunc: func(ctx context.Context, e *domain.Event) (bool, error) {
				created = true
				return false, nil // duplicate - not created
			},
		}

		logger := slog.Default()
		consumer := &Consumer{
			queue:    mockQueue,
			topic:    "test-topic",
			dlqTopic: "test-dlq",
			repo:     mockRepo,
			logger:   logger,
		}

		ctx := context.Background()
		err := consumer.consumeOne(ctx)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !created {
			t.Error("create should have been called")
		}

		if !acked {
			t.Error("message should have been acked for duplicate")
		}
	})

	t.Run("missing NextRunAt - sends to DLQ", func(t *testing.T) {
		event := &domain.Event{
			ID:             uuid.Must(uuid.NewV7()),
			IdempotencyKey: "test-key",
			Type:           "test_type",
			Payload:        map[string]any{"key": "value"},
			NextRunAt:      nil, // missing
		}
		eventData, _ := json.Marshal(event)

		var dlqPublished bool
		var acked bool

		mockQueue := &mockMessageQueueForConsumer{
			receiveFunc: func(ctx context.Context, topic string) ([]byte, error) {
				return eventData, nil
			},
			publishFunc: func(ctx context.Context, topic string, message []byte) error {
				if topic == "test-dlq" {
					dlqPublished = true
				}
				return nil
			},
			ackFunc: func(ctx context.Context) error {
				acked = true
				return nil
			},
		}

		mockRepo := &mockEventRepository{}

		logger := slog.Default()
		consumer := &Consumer{
			queue:    mockQueue,
			topic:    "test-topic",
			dlqTopic: "test-dlq",
			repo:     mockRepo,
			logger:   logger,
		}

		ctx := context.Background()
		err := consumer.consumeOne(ctx)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !dlqPublished {
			t.Error("event without NextRunAt should have been sent to DLQ")
		}

		if !acked {
			t.Error("message should have been acked after DLQ publish")
		}
	})
}

func TestConsumer_updateEvent(t *testing.T) {
	t.Run("successful update", func(t *testing.T) {
		var updated bool
		mockRepo := &mockEventRepository{
			updateFunc: func(ctx context.Context, event *domain.Event) error {
				updated = true
				return nil
			},
		}

		logger := slog.Default()
		consumer := &Consumer{
			repo:   mockRepo,
			logger: logger,
		}

		event := &domain.Event{
			ID:   uuid.Must(uuid.NewV7()),
			Type: "test_event",
		}

		ctx := context.Background()
		err := consumer.updateEvent(ctx, event)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !updated {
			t.Error("update should have been called")
		}
	})

	t.Run("update error", func(t *testing.T) {
		updateErr := errors.New("update failed")
		mockRepo := &mockEventRepository{
			updateFunc: func(ctx context.Context, event *domain.Event) error {
				return updateErr
			},
		}

		logger := slog.Default()
		consumer := &Consumer{
			repo:   mockRepo,
			logger: logger,
		}

		event := &domain.Event{
			ID:   uuid.Must(uuid.NewV7()),
			Type: "test_event",
		}

		ctx := context.Background()
		err := consumer.updateEvent(ctx, event)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if !errors.Is(err, updateErr) && err.Error() != "update event: "+updateErr.Error() {
			t.Errorf("expected update error, got %v", err)
		}
	})
}
