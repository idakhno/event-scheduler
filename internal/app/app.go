// Package app provides application initialization and lifecycle management.
package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"event_scheduler/internal/config"

	"github.com/grafana/pyroscope-go"
	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"event_scheduler/internal/adapters/messaging/kafka"
	"event_scheduler/internal/adapters/repository/postgres"
)

type App struct {
	Config        *config.Config
	Logger        *slog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	metricsServer *http.Server
}

func New(serviceName string) (*App, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if err := prometheus.Register(collectors.NewGoCollector()); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			logger.Error("register go collector", "error", err)
		}
	}
	if err := prometheus.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			logger.Error("register process collector", "error", err)
		}
	}

	if cfg.Pyroscope.Enabled {
		if err := startPyroscope(serviceName, cfg.Pyroscope); err != nil {
			logger.Warn("failed to start pyroscope", "error", err)
		} else {
			logger.Info("pyroscope started", "server", cfg.Pyroscope.ServerAddress)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{Addr: cfg.App.MetricsPort, Handler: mux}

	go func() {
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	return &App{
		Config:        cfg,
		Logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		metricsServer: metricsServer,
	}, nil
}

func (a *App) Run(runFunc func(context.Context) error) error {
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan
		a.Logger.Info("shutdown signal received")
		a.shutdown()
	}()

	return runFunc(a.ctx)
}

func (a *App) shutdown() {
	a.cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.Config.App.ShutdownTimeout)
	defer shutdownCancel()

	if err := a.metricsServer.Shutdown(shutdownCtx); err != nil {
		a.Logger.Error("failed to shutdown metrics server", "error", err)
	}
}

func startPyroscope(appName string, cfg config.PyroscopeConfig) error {
	runtime.SetMutexProfileFraction(cfg.MutexProfileFraction)
	runtime.SetBlockProfileRate(cfg.BlockProfileRate)

	_, err := pyroscope.Start(pyroscope.Config{
		ApplicationName: appName,
		ServerAddress:   cfg.ServerAddress,
		Logger:          pyroscope.StandardLogger,
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
			pyroscope.ProfileGoroutines,
			pyroscope.ProfileMutexCount,
			pyroscope.ProfileMutexDuration,
			pyroscope.ProfileBlockCount,
			pyroscope.ProfileBlockDuration,
		},
	})

	return err
}

func NewAsynqClient(cfg *config.Config) *asynq.Client {
	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	}
	return asynq.NewClient(redisOpt)
}

func NewAsynqServer(cfg *config.Config) *asynq.Server {
	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	}
	concurrency := cfg.App.AsynqConcurrency
	if concurrency == 0 {
		concurrency = runtime.NumCPU()
	}
	return asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: concurrency,
	})
}

func NewPostgresRepository(cfg *config.Config) (*postgres.EventRepository, error) {
	return postgres.NewEventRepository(
		cfg.Postgres.DSN(),
		cfg.Postgres.MaxOpenConns,
		cfg.Postgres.MaxIdleConns,
		cfg.Postgres.ConnMaxLifetime,
		cfg.Postgres.ConnMaxIdleTime,
	)
}

func NewKafkaQueue(cfg *config.Config) *kafka.MessageQueue {
	return kafka.NewMessageQueue(
		cfg.Kafka.Brokers,
		cfg.Kafka.ConsumerGroup,
		cfg.Kafka.BatchSize,
		cfg.Kafka.BatchBytes,
		cfg.Kafka.BatchTimeout,
		cfg.Kafka.MinBytes,
		cfg.Kafka.MaxBytes,
		cfg.Kafka.MaxWait,
		cfg.Kafka.RequiredAcks,
		cfg.Kafka.CommitInterval,
	)
}
