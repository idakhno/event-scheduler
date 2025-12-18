// Package postgres provides PostgreSQL implementation of the event repository.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"event_scheduler/internal/domain"
	"event_scheduler/internal/ports"

	_ "github.com/lib/pq"
)

type EventRepository struct {
	db *sql.DB
}

func NewEventRepository(dsn string, maxOpenConns, maxIdleConns int, connMaxLifetime, connMaxIdleTime time.Duration) (*EventRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetConnMaxIdleTime(connMaxIdleTime)

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &EventRepository{db: db}, nil
}

func (r *EventRepository) Get(ctx context.Context, key string) (*domain.Event, error) {
	query := `
		SELECT id, idempotency_key, type, payload, status, retry_count, next_run_at, last_error, created_at, updated_at
		FROM events
		WHERE idempotency_key = $1
	`

	var event domain.Event
	var payloadJSON []byte
	var status string

	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&event.ID,
		&event.IdempotencyKey,
		&event.Type,
		&payloadJSON,
		&status,
		&event.RetryCount,
		&event.NextRunAt,
		&event.LastError,
		&event.CreatedAt,
		&event.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	event.Status = domain.EventStatus(status)

	if err := json.Unmarshal(payloadJSON, &event.Payload); err != nil {
		return nil, err
	}

	return &event, nil
}

func (r *EventRepository) Update(ctx context.Context, event *domain.Event) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	query := `
		UPDATE events
		SET type = $2, payload = $3, status = $4, retry_count = $5, next_run_at = $6, last_error = $7, updated_at = $8
		WHERE id = $1
	`

	_, err = r.db.ExecContext(ctx, query,
		event.ID,
		event.Type,
		payloadJSON,
		string(event.Status),
		event.RetryCount,
		event.NextRunAt,
		event.LastError,
		time.Now(),
	)

	return err
}

func (r *EventRepository) Create(ctx context.Context, event *domain.Event) (bool, error) {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return false, err
	}

	query := `
		INSERT INTO events (id, idempotency_key, type, payload, status, retry_count, next_run_at, last_error, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (idempotency_key) DO NOTHING
	`

	result, err := r.db.ExecContext(ctx, query,
		event.ID,
		event.IdempotencyKey,
		event.Type,
		payloadJSON,
		string(event.Status),
		event.RetryCount,
		event.NextRunAt,
		event.LastError,
		event.CreatedAt,
		time.Now(),
	)

	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected > 0, nil
}

func (r *EventRepository) Close() error {
	return r.db.Close()
}

var _ ports.EventRepository = (*EventRepository)(nil)
