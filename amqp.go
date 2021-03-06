package amqp

import (
	"errors"

	"github.com/streadway/amqp"
)

var (
	// DefaultURL is the default amqp url to connect to.
	DefaultURL = "amqp://localhost"

	// DefaultOnDisconnect is the default callback for when AMQP gets disconnected.
	DefaultOnDisconnect = func() {
		panic("Lost connection")
	}

	// DefaultExchangeOptions are the default options used when building a new Exchange.
	DefaultExchangeOptions = &ExchangeOptions{
		Name:         "hutch",
		Type:         "topic",
		Durable:      true,
		AutoDelete:   false,
		OnDisconnect: DefaultOnDisconnect,
	}

	// DefaultQueueOptions are the default options used when building a new Queue.
	DefaultQueueOptions = &QueueOptions{
		Durable:    true,
		AutoDelete: false,
		RoutingKey: "",
	}

	ErrClosed          = amqp.ErrClosed
	ErrSASL            = amqp.ErrSASL
	ErrCredentials     = amqp.ErrCredentials
	ErrVhost           = amqp.ErrVhost
	ErrSyntax          = amqp.ErrSyntax
	ErrFrame           = amqp.ErrFrame
	ErrCommandInvalid  = amqp.ErrCommandInvalid
	ErrUnexpectedFrame = amqp.ErrUnexpectedFrame
	ErrFieldType       = amqp.ErrFieldType
)

// ExchangeOptions can be passed to NewExchange to configure the Exchange.
// If the connection is lost then OnDisconnect is called. OnDisconnect returns whether or not to
// continue processing.
type ExchangeOptions struct {
	Name         string
	Type         string
	Durable      bool
	AutoDelete   bool
	OnDisconnect func()
}

// QueueOptions can be passed to NewQueue to configure the queue.
type QueueOptions struct {
	Durable       bool
	AutoDelete    bool
	RoutingKey    string
	PrefetchCount int
	PrefetchSize  int
}

// Exchange represents an amqp exchange and wraps an amqp.Connection
// and an amqp.Channel.
type Exchange struct {
	Name         string
	connection   *amqp.Connection
	channel      *amqp.Channel
	onDisconnect func()
}

// NewExchange connects to rabbitmq, opens a channel and returns a new
// Exchange instance. If url is an empty string, it will attempt to connect
// to localhost.
func NewExchange(url string, options *ExchangeOptions) (*Exchange, error) {
	if url == "" {
		url = DefaultURL
	}

	if options == nil {
		options = DefaultExchangeOptions
	}

	c, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}

	if options.OnDisconnect == nil {
		options.OnDisconnect = DefaultOnDisconnect
	}

	err = ch.ExchangeDeclare(
		options.Name,       // name
		options.Type,       // kind
		options.Durable,    // durable
		options.AutoDelete, // autoDelete
		false,              // internal
		false,              // noWait
		nil,                // args
	)
	if err != nil {
		return nil, err
	}

	return &Exchange{
		Name:         options.Name,
		connection:   c,
		channel:      ch,
		onDisconnect: options.OnDisconnect,
	}, nil
}

// Publish publishes a persistent message to the Exchange.
func (e *Exchange) Publish(routingKey, message, requestID string) error {
	return e.publish(routingKey, message, requestID, amqp.Persistent)
}

// PublishTransient publishes a transient message to the Exchange.
func (e *Exchange) PublishTransient(routingKey, message, requestID string) error {
	return e.publish(routingKey, message, requestID, amqp.Transient)
}

func (e *Exchange) publish(routingKey, message, requestID string, deliveryMode uint8) error {
	if e.channel == nil {
		return errors.New("channel is nil")
	}

	msg := amqp.Publishing{
		Headers: amqp.Table{
			"request_id": requestID,
		},
		ContentType:  "application/json",
		Body:         []byte(message),
		DeliveryMode: deliveryMode,
		Priority:     0,
	}

	return e.channel.Publish(
		e.Name,     // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // imediate
		msg,        // message
	)
}

// Close closes the connection.
func (e *Exchange) Close() error {
	if err := e.channel.Close(); err != nil {
		return err
	}

	return e.connection.Close()
}

// Queue represents an amqp queue.
type Queue struct {
	exchange   *Exchange
	routingKey string
	name       string
}

// NewQueue returns a new Queue instance.
func NewQueue(queueName string, exchange *Exchange, options *QueueOptions) (*Queue, error) {
	if options == nil {
		options = DefaultQueueOptions
	}

	_, err := exchange.channel.QueueDeclare(
		queueName,          // name
		options.Durable,    // durable
		options.AutoDelete, // autoDelete
		false,              // exclusive
		false,              // noWait
		nil,                // args
	)
	if err != nil {
		return nil, err
	}

	if options.PrefetchCount > 0 || options.PrefetchSize > 0 {
		err = exchange.channel.Qos(options.PrefetchCount, options.PrefetchSize, false)
		if err != nil {
			return nil, err
		}
	}

	return &Queue{
		exchange:   exchange,
		routingKey: options.RoutingKey,
		name:       queueName,
	}, nil
}

// Purge purges all messages in the queue.
func (q *Queue) Purge() error {
	_, err := q.exchange.channel.QueuePurge(q.name, false)
	return err
}

// Name returns the name of the queue.
func (q *Queue) Name() string {
	return q.name
}

// Subscribe starts consuming from the queue.
func (q *Queue) Subscribe(messages chan<- *Message) error {
	if err := q.bind(); err != nil {
		return err
	}

	dd, err := q.exchange.channel.Consume(
		q.name,           // queue
		q.consumerName(), // consumer name
		false,            // autoAck
		false,            // exclusive
		false,            // noLocal
		false,            // noWait
		nil,              // args
	)
	if err != nil {
		return err
	}

	go func() {
		open := true
		for open {
			select {
			case d, ok := <-dd:
				if !ok {
					q.exchange.onDisconnect()
					open = false
					break
				}

				m := &Message{
					Acknowledger: &acknowledger{
						Acknowledger: d.Acknowledger,
						deliveryTag:  d.DeliveryTag,
					},
					Headers: d.Headers,
					Body:    d.Body,
				}

				messages <- m
			}
		}
	}()

	return nil
}

// Close closes the exchange.
func (q *Queue) Close() error {
	if err := q.exchange.channel.Cancel(q.consumerName(), false); err != nil {
		return err
	}

	ch := q.exchange.channel.NotifyClose(make(chan *amqp.Error))
	q.exchange.Close()

	// Wait for the deliveries to drain.
	if err := <-ch; err != nil {
		return err
	}

	return nil
}

// bind binds the queue. This is called automatically when Subscribe is called.
func (q *Queue) bind() error {
	return q.exchange.channel.QueueBind(
		q.name,          // name
		q.routingKey,    // key
		q.exchange.Name, // exchange
		false,           // noWait
		nil,             // args
	)
}

func (q *Queue) consumerName() string {
	return q.name
}

// Message represents an amqp message.
type Message struct {
	Acknowledger
	Headers map[string]interface{}
	Body    []byte
}

// Acknowledger allows a message to be acked or nacked (rejected).
type Acknowledger interface {
	Ack() error
	Nack(requeue bool) error
}

// acknowledger wraps an amqp.Acknowledger to implement the Acknowledger interface.
type acknowledger struct {
	amqp.Acknowledger
	deliveryTag uint64
}

// Ack implements Acknowledger Ack.
func (d *acknowledger) Ack() error {
	return d.Acknowledger.Ack(d.deliveryTag, false)
}

// Nack implements Acknowledger Nack.
func (d *acknowledger) Nack(requeue bool) error {
	return d.Acknowledger.Nack(d.deliveryTag, false, requeue)
}

// Acknowledgement specifieds an acknowledgement type.
type Acknowledgement int

func (a Acknowledgement) String() string {
	switch a {
	case Acked:
		return "acked"
	case Requeued:
		return "requeued"
	case Dropped:
		return "dropped"
	}

	return "unacknowledged"
}

// Used with the NullAcknowledger to determine the Acknowledgement type.
const (
	Unacknowledged Acknowledgement = iota
	Acked
	Requeued
	Dropped
)

// ErrAlreadyAcked is returned by the NullAcknowledger if the message has already been
// acked.
var ErrAlreadyAcked = errors.New("already acked")

// NullAcknowledger is an implementation of the amqp.Acknowledger interface that
// stores the acknowledgement in a variable.
type NullAcknowledger struct {
	acked           bool
	Acknowledgement Acknowledgement
}

// Ack sets acked to true.
func (a *NullAcknowledger) Ack() error {
	if a.acked {
		return ErrAlreadyAcked
	}

	a.acked = true
	a.Acknowledgement = Acked
	return nil
}

// Nack sets nacked to true.
func (a *NullAcknowledger) Nack(requeue bool) error {
	if a.acked {
		return ErrAlreadyAcked
	}

	a.acked = true
	if requeue {
		a.Acknowledgement = Requeued
	} else {
		a.Acknowledgement = Dropped
	}
	return nil
}
