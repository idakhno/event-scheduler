// Package domain defines core business entities and their behavior.
package domain

import (
	"time"

	"github.com/google/uuid"
)

type EventStatus string

const (
	StatusScheduled  EventStatus = "scheduled"
	StatusProcessing EventStatus = "processing"
	StatusDone       EventStatus = "done"
	StatusFailed     EventStatus = "failed"
	StatusDead       EventStatus = "dead"
)

type Event struct {
	ID             uuid.UUID
	IdempotencyKey string
	Type           string
	Payload        map[string]any
	Status         EventStatus
	RetryCount     int
	NextRunAt      *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func NewEvent(idempotencyKey, eventType string, payload map[string]any, runAt time.Time) *Event {
	now := time.Now()
	return &Event{
		ID:             uuid.Must(uuid.NewV7()),
		IdempotencyKey: idempotencyKey,
		Type:           eventType,
		Payload:        payload,
		Status:         StatusScheduled,
		NextRunAt:      &runAt,
		RetryCount:     0,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func (e *Event) MarkAsProcessing() {
	e.Status = StatusProcessing
	e.updateTimestamp()
}

func (e *Event) MarkAsDone() {
	e.Status = StatusDone
	e.updateTimestamp()
}

func (e *Event) MarkAsFailed(err error) {
	e.Status = StatusFailed
	if err != nil {
		errMsg := err.Error()
		e.LastError = &errMsg
	}
	e.RetryCount++
	e.updateTimestamp()
}

func (e *Event) MarkAsDead() {
	e.Status = StatusDead
	e.updateTimestamp()
}

func (e *Event) updateTimestamp() {
	e.UpdatedAt = time.Now()
}
