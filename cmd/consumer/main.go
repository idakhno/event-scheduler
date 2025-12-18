package main

import (
	"context"
	"os"

	"event_scheduler/internal/app"
	"event_scheduler/internal/usecase"
)

func main() {
	application, err := app.New("consumer")
	if err != nil {
		os.Exit(1)
	}

	cfg := application.Config

	repo, err := app.NewPostgresRepository(cfg)
	if err != nil {
		application.Logger.Error("postgres connection failed", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	queue := app.NewKafkaQueue(cfg)
	defer queue.Close()

	asynqClient := app.NewAsynqClient(cfg)
	defer asynqClient.Close()

	asynqServer := app.NewAsynqServer(cfg)

	consumer := usecase.NewConsumer(
		queue,
		cfg.Kafka.Topic,
		cfg.Kafka.DLQTopic,
		repo,
		asynqClient,
		asynqServer,
		cfg.App.MaxRetries,
		cfg.App.TaskErrorChance,
		cfg.App.TaskExecutionDuration,
		cfg.App.ShutdownTimeout,
		application.Logger,
	)

	application.Run(func(ctx context.Context) error {
		return consumer.Run(ctx)
	})
}
