// Package ports defines interfaces for external dependencies.
package ports

import (
	"context"

	"event_scheduler/internal/domain"
)

type EventRepository interface {
	Get(ctx context.Context, key string) (*domain.Event, error)
	Create(ctx context.Context, event *domain.Event) (bool, error)
	Update(ctx context.Context, event *domain.Event) error
}
