package amqptest

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/remind101/amqp"
)

func TestSimplePubSubAck(t *testing.T) {
	key := "amqptest.messages"
	numMessages := 3
	q, ch := Subscribe(t, "amqptest.queue", key, nil)
	defer q.Close()

	// Publish messages
	for i := 0; i < numMessages; i++ {
		Publish(t, key, fmt.Sprintf("%d", i))
	}

	// Consume messages
	messages := make([]string, numMessages)
	for i := 0; i < numMessages; i++ {
		m := <-ch
		messages[i] = string(m.Body)
		m.Ack()
	}

	if got, want := strings.Join(messages, ","), "0,1,2"; got != want {
		t.Errorf("Received %q; want %q", got, want)
	}
}

func TestPrefetchCount(t *testing.T) {
	key := "amqptest.messages"
	numMessages := 30
	q, _ := Subscribe(t, "amqptest.queue", key, &amqp.QueueOptions{
		Durable:       true,
		AutoDelete:    true,
		PrefetchCount: 10,
	})
	defer q.Close()

	// Publish messages
	for i := 0; i < numMessages; i++ {
		Publish(t, key, fmt.Sprintf("%d", i))
	}

	// fmt.Printf("msg: %b", <-ch)
	s, err := q.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("queue size: %d\n", s.Messages)
	time.Sleep(30 * time.Second)
}
