package amqp

import (
	"testing"

	"github.com/streadway/amqp"
)

func TestErrorsAreEqual(t *testing.T) {
	if ErrClosed != amqp.ErrClosed {
		t.Fatalf("Expected errors to be equal but: %v != %v", ErrClosed, amqp.ErrClosed)
	}
}
