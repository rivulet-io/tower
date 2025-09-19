package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func (c *conn) SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte) ([]byte, bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.conn.Subscribe(subject, func(msg *nats.Msg) {
		response, ok := handler(msg.Subject, msg.Data)
		if !ok || msg.Reply == "" {
			return
		}
		if err := msg.Respond(response); err != nil {
			errHandler(fmt.Errorf("failed to respond to message on subject %q: %w", msg.Subject, err))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	return func() {
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from subject %q: %w", subject, err))
		}
	}, nil
}

func (c *conn) SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte) ([]byte, bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		response, ok := handler(msg.Subject, msg.Data)
		if !ok || msg.Reply == "" {
			return
		}
		if err := msg.Respond(response); err != nil {
			errHandler(fmt.Errorf("failed to respond to message on subject %q: %w", msg.Subject, err))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	return func() {
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from subject %q: %w", subject, err))
		}
	}, nil
}

func (c *conn) PublishVolatile(subject string, msg []byte) error {
	if err := c.conn.Publish(subject, msg); err != nil {
		return fmt.Errorf("failed to publish to subject %q: %w", subject, err)
	}

	return nil
}

func (c *conn) RequestVolatile(subject string, msg []byte, timeout time.Duration) ([]byte, error) {
	response, err := c.conn.Request(subject, msg, timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to request on subject %q: %w", subject, err)
	}

	return response.Data, nil
}
