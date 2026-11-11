package broker

import (
	"context"
	"errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"net"
	"os"
	"soup-job-queue/internal/telemetry"
	"time"
)

const Queue = "soup.jobs"

type Session struct {
	Conn     *amqp.Connection
	Channel  *amqp.Channel
	socket   net.Conn
	returns  <-chan amqp.Return
	confirms <-chan amqp.Confirmation
}

func Open() (*Session, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://soup:local-development-only@rabbitmq:5672/"
	}
	var socket net.Conn
	c, e := amqp.DialConfig(url, amqp.Config{Heartbeat: 5 * time.Second, Dial: func(network, address string) (net.Conn, error) {
		var err error
		socket, err = amqp.DefaultDial(5*time.Second)(network, address)
		return socket, err
	}})
	if e != nil {
		return nil, e
	}
	ch, e := c.Channel()
	if e != nil {
		c.Close()
		return nil, e
	}
	s := &Session{Conn: c, Channel: ch, socket: socket}
	fail := func(e error) (*Session, error) { s.Close(); return nil, e }
	if e = ch.ExchangeDeclare("soup.dead", "direct", true, false, false, false, nil); e != nil {
		return fail(e)
	}
	if _, e = ch.QueueDeclare("soup.dead", true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); e != nil {
		return fail(e)
	}
	if e = ch.QueueBind("soup.dead", "dead", "soup.dead", false, nil); e != nil {
		return fail(e)
	}
	_, e = ch.QueueDeclare(Queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int32(20), "x-dead-letter-exchange": "soup.dead", "x-dead-letter-routing-key": "dead"})
	if e != nil {
		return fail(e)
	}
	return s, nil
}
func (s *Session) Close() { _ = s.Conn.CloseDeadline(time.Now().Add(time.Second)) }

// One outstanding publish per channel makes returns/confirms unambiguous.
func (s *Session) Publish(ctx context.Context, body []byte) error {
	if s.confirms == nil {
		if e := s.Channel.Confirm(false); e != nil {
			return e
		}
		s.returns = s.Channel.NotifyReturn(make(chan amqp.Return, 1))
		s.confirms = s.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	}
	deadline, _ := ctx.Deadline()
	_ = s.socket.SetWriteDeadline(deadline)
	defer s.socket.SetWriteDeadline(time.Time{})
	h := amqp.Table{}
	for k, v := range telemetry.Carrier(ctx) {
		h[k] = v
	}
	if e := s.Channel.PublishWithContext(ctx, "", Queue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, ContentType: "application/json", Headers: h, Body: body}); e != nil {
		return e
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.returns:
		return errors.New("unroutable publish")
	case c, ok := <-s.confirms:
		if !ok || !c.Ack {
			return errors.New("unconfirmed publish")
		}
		select {
		case <-s.returns:
			return errors.New("returned publish")
		default:
			return nil
		}
	}
}
