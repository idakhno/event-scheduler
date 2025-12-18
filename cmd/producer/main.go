package main

import (
	"context"
	"os"

	"event_scheduler/internal/app"
	"event_scheduler/internal/usecase"
)

func main() {
	application, err := app.New("producer")
	if err != nil {
		os.Exit(1)
	}

	cfg := application.Config

	queue := app.NewKafkaQueue(cfg)
	defer queue.Close()

	producer := usecase.NewProducer(
		queue,
		cfg.Kafka.Topic,
		cfg.App.GetEventTypes(),
		cfg.App.PayloadMaxValue,
		cfg.App.EventDelayMinSeconds,
		cfg.App.EventDelayMaxSeconds,
		application.Logger,
	)

	application.Run(func(ctx context.Context) error {
		return producer.Run(ctx, cfg.App.ProducerInterval)
	})
}
