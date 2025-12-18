// Package config provides configuration loading from environment variables.
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Postgres  PostgresConfig
	Kafka     KafkaConfig
	Redis     RedisConfig
	App       AppConfig
	Pyroscope PyroscopeConfig
}

type PostgresConfig struct {
	Host            string        `envconfig:"POSTGRES_HOST" default:"localhost"`
	Port            int           `envconfig:"POSTGRES_PORT" default:"5432"`
	User            string        `envconfig:"POSTGRES_USER" default:"postgres"`
	Password        string        `envconfig:"POSTGRES_PASSWORD"`
	Database        string        `envconfig:"POSTGRES_DB" default:"event_scheduler"`
	SSLMode         string        `envconfig:"POSTGRES_SSLMODE" default:"disable"`
	MaxOpenConns    int           `envconfig:"POSTGRES_MAX_OPEN_CONNS" default:"25"`
	MaxIdleConns    int           `envconfig:"POSTGRES_MAX_IDLE_CONNS" default:"5"`
	ConnMaxLifetime time.Duration `envconfig:"POSTGRES_CONN_MAX_LIFETIME" default:"5m"`
	ConnMaxIdleTime time.Duration `envconfig:"POSTGRES_CONN_MAX_IDLE_TIME" default:"1m"`
}

func (c PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode)
}

type KafkaConfig struct {
	Brokers        []string      `envconfig:"KAFKA_BROKERS" default:"localhost:9092"`
	Topic          string        `envconfig:"KAFKA_TOPIC" default:"events"`
	ConsumerGroup  string        `envconfig:"KAFKA_CONSUMER_GROUP" default:"event-consumer"`
	DLQTopic       string        `envconfig:"KAFKA_DLQ_TOPIC" default:"events-dlq"`
	BatchSize      int           `envconfig:"KAFKA_BATCH_SIZE" default:"100"`
	BatchBytes     int64         `envconfig:"KAFKA_BATCH_BYTES" default:"1048576"`
	BatchTimeout   time.Duration `envconfig:"KAFKA_BATCH_TIMEOUT" default:"10ms"`
	MinBytes       int           `envconfig:"KAFKA_MIN_BYTES" default:"10000"`
	MaxBytes       int           `envconfig:"KAFKA_MAX_BYTES" default:"10000000"`
	MaxWait        time.Duration `envconfig:"KAFKA_MAX_WAIT" default:"1s"`
	RequiredAcks   int           `envconfig:"KAFKA_REQUIRED_ACKS" default:"1"`
	CommitInterval time.Duration `envconfig:"KAFKA_COMMIT_INTERVAL" default:"0"`
}

type RedisConfig struct {
	Addr     string `envconfig:"REDIS_ADDR" default:"localhost:6379"`
	Password string `envconfig:"REDIS_PASSWORD"`
	DB       int    `envconfig:"REDIS_DB" default:"0"`
}

type AppConfig struct {
	MaxRetries            int           `envconfig:"APP_MAX_RETRIES" default:"3"`
	ProducerInterval      time.Duration `envconfig:"APP_PRODUCER_INTERVAL" default:"2s"`
	MetricsPort           string        `envconfig:"METRICS_PORT" default:":8080"`
	ShutdownTimeout       time.Duration `envconfig:"APP_SHUTDOWN_TIMEOUT" default:"5s"`
	AsynqConcurrency      int           `envconfig:"APP_ASYNQ_CONCURRENCY" default:"0"`
	EventTypes            string        `envconfig:"APP_EVENT_TYPES" default:"user.created,order.placed,payment.processed,notification.sent"`
	PayloadMaxValue       int           `envconfig:"APP_PAYLOAD_MAX_VALUE" default:"1000"`
	EventDelayMinSeconds  int           `envconfig:"APP_EVENT_DELAY_MIN_SECONDS" default:"1"`
	EventDelayMaxSeconds  int           `envconfig:"APP_EVENT_DELAY_MAX_SECONDS" default:"10"`
	TaskErrorChance       int           `envconfig:"APP_TASK_ERROR_CHANCE" default:"10"`
	TaskExecutionDuration time.Duration `envconfig:"APP_TASK_EXECUTION_DURATION" default:"50ms"`
}

type PyroscopeConfig struct {
	ServerAddress        string `envconfig:"PYROSCOPE_SERVER" default:"http://localhost:4040"`
	Enabled              bool   `envconfig:"PYROSCOPE_ENABLED" default:"true"`
	MutexProfileFraction int    `envconfig:"PYROSCOPE_MUTEX_PROFILE_FRACTION" default:"5"`
	BlockProfileRate     int    `envconfig:"PYROSCOPE_BLOCK_PROFILE_RATE" default:"5"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *AppConfig) GetEventTypes() []string {
	if c.EventTypes == "" {
		return []string{}
	}
	types := strings.Split(c.EventTypes, ",")
	for i := range types {
		types[i] = strings.TrimSpace(types[i])
	}
	return types
}
