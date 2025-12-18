// Package ports defines interfaces for external dependencies.
package ports

import "context"

type MessageQueue interface {
	Publish(ctx context.Context, topic string, message []byte) error
	Receive(ctx context.Context, topic string) ([]byte, error)
	Ack(ctx context.Context) error
	Close() error
}
