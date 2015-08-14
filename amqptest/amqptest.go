package amqptest

import (
	"testing"

	"code.google.com/p/go-uuid/uuid"
	"github.com/remind101/amqp"
)

var Exchange *amqp.Exchange

func Publish(t *testing.T, route, message string) {
	if Exchange == nil {
		Exchange = newTestExchange(t)
	}

	if err := Exchange.Publish(route, message, uuid.New()); err != nil {
		t.Error(err)
	}
}

func newTestExchange(t *testing.T) *amqp.Exchange {
	e, err := amqp.NewExchange("", &amqp.ExchangeOptions{
		Name:         "hutch",
		Type:         "topic",
		Durable:      true,
		AutoDelete:   false,
		OnDisconnect: func() bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func Subscribe(t *testing.T, queueName string, route string, qo *amqp.QueueOptions) (*amqp.Queue, chan *amqp.Message) {
	if qo == nil {
		qo = &amqp.QueueOptions{
			Durable:    true,
			AutoDelete: true,
		}
	}
	qo.RoutingKey = route

	e := newTestExchange(t)

	q, err := amqp.NewQueue(queueName, e, qo)
	if err != nil {
		t.Fatal(err)
	}

	messages := make(chan *amqp.Message)
	q.Subscribe(messages)
	return q, messages
}
