package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewEvent(t *testing.T) {
	idempotencyKey := "test-key-123"
	eventType := "test_event"
	payload := map[string]any{
		"key": "value",
		"num": 42,
	}
	runAt := time.Now().Add(5 * time.Minute)

	event := NewEvent(idempotencyKey, eventType, payload, runAt)

	if event == nil {
		t.Fatal("NewEvent returned nil")
	}

	if event.IdempotencyKey != idempotencyKey {
		t.Errorf("expected IdempotencyKey %q, got %q", idempotencyKey, event.IdempotencyKey)
	}

	if event.Type != eventType {
		t.Errorf("expected Type %q, got %q", eventType, event.Type)
	}

	if event.Status != StatusScheduled {
		t.Errorf("expected Status %q, got %q", StatusScheduled, event.Status)
	}

	if event.RetryCount != 0 {
		t.Errorf("expected RetryCount 0, got %d", event.RetryCount)
	}

	if event.NextRunAt == nil {
		t.Fatal("NextRunAt is nil")
	}

	if !event.NextRunAt.Equal(runAt) {
		t.Errorf("expected NextRunAt %v, got %v", runAt, *event.NextRunAt)
	}

	if event.ID == (uuid.UUID{}) {
		t.Error("ID should not be empty")
	}

	if event.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	if event.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}
}

func TestEvent_MarkAsProcessing(t *testing.T) {
	event := &Event{
		Status:    StatusScheduled,
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}

	beforeUpdate := time.Now()
	event.MarkAsProcessing()
	afterUpdate := time.Now()

	if event.Status != StatusProcessing {
		t.Errorf("expected Status %q, got %q", StatusProcessing, event.Status)
	}

	if event.UpdatedAt.Before(beforeUpdate) || event.UpdatedAt.After(afterUpdate) {
		t.Errorf("UpdatedAt should be updated, got %v", event.UpdatedAt)
	}
}

func TestEvent_MarkAsDone(t *testing.T) {
	event := &Event{
		Status:    StatusProcessing,
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}

	beforeUpdate := time.Now()
	event.MarkAsDone()
	afterUpdate := time.Now()

	if event.Status != StatusDone {
		t.Errorf("expected Status %q, got %q", StatusDone, event.Status)
	}

	if event.UpdatedAt.Before(beforeUpdate) || event.UpdatedAt.After(afterUpdate) {
		t.Errorf("UpdatedAt should be updated, got %v", event.UpdatedAt)
	}
}

func TestEvent_MarkAsFailed(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		event := &Event{
			Status:     StatusProcessing,
			RetryCount: 0,
			UpdatedAt:  time.Now().Add(-1 * time.Hour),
		}
		testErr := &testError{msg: "test error"}

		beforeUpdate := time.Now()
		event.MarkAsFailed(testErr)
		afterUpdate := time.Now()

		if event.Status != StatusFailed {
			t.Errorf("expected Status %q, got %q", StatusFailed, event.Status)
		}

		if event.RetryCount != 1 {
			t.Errorf("expected RetryCount 1, got %d", event.RetryCount)
		}

		if event.LastError == nil {
			t.Fatal("LastError should not be nil")
		}

		if *event.LastError != testErr.Error() {
			t.Errorf("expected LastError %q, got %q", testErr.Error(), *event.LastError)
		}

		if event.UpdatedAt.Before(beforeUpdate) || event.UpdatedAt.After(afterUpdate) {
			t.Errorf("UpdatedAt should be updated, got %v", event.UpdatedAt)
		}
	})

	t.Run("without error", func(t *testing.T) {
		event := &Event{
			Status:     StatusProcessing,
			RetryCount: 2,
			UpdatedAt:  time.Now().Add(-1 * time.Hour),
		}

		event.MarkAsFailed(nil)

		if event.Status != StatusFailed {
			t.Errorf("expected Status %q, got %q", StatusFailed, event.Status)
		}

		if event.RetryCount != 3 {
			t.Errorf("expected RetryCount 3, got %d", event.RetryCount)
		}

		if event.LastError != nil {
			t.Error("LastError should be nil when error is nil")
		}
	})
}

func TestEvent_MarkAsDead(t *testing.T) {
	event := &Event{
		Status:    StatusFailed,
		UpdatedAt: time.Now().Add(-1 * time.Hour),
	}

	beforeUpdate := time.Now()
	event.MarkAsDead()
	afterUpdate := time.Now()

	if event.Status != StatusDead {
		t.Errorf("expected Status %q, got %q", StatusDead, event.Status)
	}

	if event.UpdatedAt.Before(beforeUpdate) || event.UpdatedAt.After(afterUpdate) {
		t.Errorf("UpdatedAt should be updated, got %v", event.UpdatedAt)
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
