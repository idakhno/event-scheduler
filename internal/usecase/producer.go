// Package usecase implements business logic for event production and consumption.
package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"event_scheduler/internal/domain"
	"event_scheduler/internal/ports"

	"github.com/google/uuid"
)

type Producer struct {
	queue           ports.MessageQueue
	topic           string
	eventTypes      []string
	payloadMaxValue int
	delayMinSeconds int
	delayMaxSeconds int
	logger          *slog.Logger
}

func NewProducer(
	queue ports.MessageQueue,
	topic string,
	eventTypes []string,
	payloadMaxValue, delayMinSeconds, delayMaxSeconds int,
	logger *slog.Logger,
) *Producer {
	return &Producer{
		queue:           queue,
		topic:           topic,
		eventTypes:      eventTypes,
		payloadMaxValue: payloadMaxValue,
		delayMinSeconds: delayMinSeconds,
		delayMaxSeconds: delayMaxSeconds,
		logger:          logger,
	}
}

func (p *Producer) Run(ctx context.Context, interval time.Duration) error {
	p.logger.Info("producer started", "topic", p.topic, "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("producer stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := p.produceEvent(ctx); err != nil {
				p.logger.Error("failed to produce event", "error", err)
			}
		}
	}
}

func (p *Producer) produceEvent(ctx context.Context) error {
	eventType := p.eventTypes[rand.Intn(len(p.eventTypes))]
	payload := map[string]any{
		"timestamp": time.Now().Unix(),
		"value":     rand.Intn(p.payloadMaxValue),
	}

	idempotencyKey := fmt.Sprintf("%s-%s", eventType, uuid.Must(uuid.NewV7()).String())
	delaySeconds := p.delayMinSeconds + rand.Intn(p.delayMaxSeconds-p.delayMinSeconds+1)
	runAt := time.Now().Add(time.Duration(delaySeconds) * time.Second)

	event := domain.NewEvent(idempotencyKey, eventType, payload, runAt)

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.queue.Publish(ctx, p.topic, data); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	p.logger.Info("event produced", "id", event.ID, "type", event.Type, "run_at", runAt)
	return nil
}
