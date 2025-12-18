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

	"github.com/hibiken/asynq"
)

const taskTypeProcessEvent = "event:process"

type Consumer struct {
	queue                 ports.MessageQueue
	topic                 string
	dlqTopic              string
	repo                  ports.EventRepository
	asynqClient           *asynq.Client
	asynqServer           *asynq.Server
	maxRetries            int
	taskErrorChance       int
	taskExecutionDuration time.Duration
	shutdownTimeout       time.Duration
	logger                *slog.Logger
}

func NewConsumer(
	queue ports.MessageQueue,
	topic, dlqTopic string,
	repo ports.EventRepository,
	asynqClient *asynq.Client,
	asynqServer *asynq.Server,
	maxRetries int,
	taskErrorChance int,
	taskExecutionDuration time.Duration,
	shutdownTimeout time.Duration,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		queue:                 queue,
		topic:                 topic,
		dlqTopic:              dlqTopic,
		repo:                  repo,
		asynqClient:           asynqClient,
		asynqServer:           asynqServer,
		maxRetries:            maxRetries,
		taskErrorChance:       taskErrorChance,
		taskExecutionDuration: taskExecutionDuration,
		shutdownTimeout:       shutdownTimeout,
		logger:                logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer started", "topic", c.topic)

	mux := asynq.NewServeMux()
	mux.HandleFunc(taskTypeProcessEvent, c.HandleEventTask)

	serverErr := make(chan error, 1)
	go func() {
		if err := c.asynqServer.Run(mux); err != nil {
			serverErr <- err
		} else {
			serverErr <- nil
		}
	}()

	go c.consumeLoop(ctx)

	select {
	case <-ctx.Done():
		c.logger.Info("shutdown signal received, stopping asynq server")
		c.asynqServer.Shutdown()
		select {
		case <-serverErr:
		case <-time.After(c.shutdownTimeout):
			c.logger.Warn("asynq server shutdown timeout")
		}
		c.logger.Info("consumer stopped")
		return ctx.Err()
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("asynq server: %w", err)
		}
		return nil
	}
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := c.consumeOne(ctx); err != nil {
				c.logger.Error("consume error", "error", err)
			}
		}
	}
}

func (c *Consumer) consumeOne(ctx context.Context) error {
	data, err := c.queue.Receive(ctx, c.topic)
	if err != nil {
		return fmt.Errorf("receive: %w", err)
	}

	var event domain.Event
	if err := json.Unmarshal(data, &event); err != nil {
		if dlqErr := c.queue.Publish(ctx, c.dlqTopic, data); dlqErr != nil {
			return fmt.Errorf("unmarshal: %w; dlq_publish: %v", err, dlqErr)
		}
		if ackErr := c.queue.Ack(ctx); ackErr != nil {
			return fmt.Errorf("ack after dlq publish: %w", ackErr)
		}
		return fmt.Errorf("unmarshal: %w", err)
	}

	if event.NextRunAt == nil {
		if dlqErr := c.queue.Publish(ctx, c.dlqTopic, data); dlqErr != nil {
			return fmt.Errorf("missing run time; dlq_publish: %v", dlqErr)
		}
		if ackErr := c.queue.Ack(ctx); ackErr != nil {
			return fmt.Errorf("ack after missing run time: %w", ackErr)
		}
		return fmt.Errorf("event has no run time")
	}

	created, err := c.repo.Create(ctx, &event)
	if err != nil {
		return fmt.Errorf("create event: %w", err)
	}

	if !created {
		c.logger.Debug("duplicate event", "key", event.IdempotencyKey)
		if err := c.queue.Ack(ctx); err != nil {
			return fmt.Errorf("ack duplicate: %w", err)
		}
		return nil
	}

	if err := c.enqueueTask(ctx, event, data); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}

	if err := c.queue.Ack(ctx); err != nil {
		return fmt.Errorf("ack after schedule: %w", err)
	}
	c.logger.Info("event scheduled", "id", event.ID, "type", event.Type, "run_at", *event.NextRunAt)
	return nil
}

func (c *Consumer) HandleEventTask(ctx context.Context, task *asynq.Task) error {
	var event domain.Event
	if err := json.Unmarshal(task.Payload(), &event); err != nil {
		if dlqErr := c.queue.Publish(ctx, c.dlqTopic, task.Payload()); dlqErr != nil {
			c.logger.Warn("failed to publish corrupted payload to DLQ", "error", dlqErr)
		}
		return asynq.SkipRetry
	}

	dbEvent, err := c.repo.Get(ctx, event.IdempotencyKey)
	if err != nil {
		return fmt.Errorf("get event: %w", err)
	}
	if dbEvent == nil {
		c.logger.Warn("event not found in database, skipping", "key", event.IdempotencyKey)
		return nil
	}

	dbEvent.MarkAsProcessing()
	if err := c.updateEvent(ctx, dbEvent); err != nil {
		return fmt.Errorf("update processing: %w", err)
	}

	if err := c.executeTask(dbEvent); err != nil {
		if attempt, ok := asynq.GetRetryCount(ctx); ok {
			dbEvent.RetryCount = attempt
		}
		if dbEvent.RetryCount+1 >= c.maxRetries {
			dbEvent.MarkAsDead()
			if updateErr := c.updateEvent(ctx, dbEvent); updateErr != nil {
				return fmt.Errorf("update dead: %w", updateErr)
			}
			if dlqErr := c.queue.Publish(ctx, c.dlqTopic, task.Payload()); dlqErr != nil {
				return fmt.Errorf("dlq publish: %w", dlqErr)
			}
			c.logger.Warn("event moved to DLQ", "id", dbEvent.ID, "type", dbEvent.Type)
			return asynq.SkipRetry
		}

		dbEvent.MarkAsFailed(err)
		if updateErr := c.updateEvent(ctx, dbEvent); updateErr != nil {
			return fmt.Errorf("update failed status: %w", updateErr)
		}
		return err
	}

	dbEvent.MarkAsDone()
	if err := c.updateEvent(ctx, dbEvent); err != nil {
		return fmt.Errorf("update done: %w", err)
	}

	c.logger.Info("event processed", "id", dbEvent.ID, "type", dbEvent.Type)
	return nil
}

func (c *Consumer) enqueueTask(ctx context.Context, event domain.Event, data []byte) error {
	task := asynq.NewTask(
		taskTypeProcessEvent,
		data,
		asynq.MaxRetry(c.maxRetries),
		asynq.ProcessAt(*event.NextRunAt),
		asynq.TaskID(event.IdempotencyKey),
	)
	if _, err := c.asynqClient.EnqueueContext(ctx, task); err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}
	return nil
}

func (c *Consumer) updateEvent(ctx context.Context, event *domain.Event) error {
	if err := c.repo.Update(ctx, event); err != nil {
		return fmt.Errorf("update event: %w", err)
	}
	return nil
}

func (c *Consumer) executeTask(event *domain.Event) error {
	c.logger.Debug("executing task", "id", event.ID, "type", event.Type)
	if c.taskErrorChance > 0 && rand.Intn(c.taskErrorChance) == 0 {
		return fmt.Errorf("random failure")
	}
	time.Sleep(c.taskExecutionDuration)
	return nil
}
