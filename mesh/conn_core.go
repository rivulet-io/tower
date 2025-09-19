package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func (c *conn) SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.conn.Subscribe(subject, func(msg *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				errHandler(fmt.Errorf("handler panic on subject %q: %v", msg.Subject, r))
			}
		}()

		response, responseHeaders, ok := handler(msg.Subject, msg.Data, msg.Header)
		if !ok || msg.Reply == "" {
			return
		}

		respMsg := nats.NewMsg(msg.Reply)
		respMsg.Data = response
		respMsg.Header = responseHeaders

		if err := c.conn.PublishMsg(respMsg); err != nil {
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

func (c *conn) SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.conn.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		defer func() {
			if r := recover(); r != nil {
				errHandler(fmt.Errorf("handler panic on subject %q (queue: %s): %v", msg.Subject, queue, r))
			}
		}()

		response, responseHeaders, ok := handler(msg.Subject, msg.Data, msg.Header)
		if !ok || msg.Reply == "" {
			return
		}

		respMsg := nats.NewMsg(msg.Reply)
		respMsg.Data = response
		respMsg.Header = responseHeaders

		if err := c.conn.PublishMsg(respMsg); err != nil {
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

func (c *conn) PublishVolatile(subject string, msg []byte, headers ...nats.Header) error {
	m := nats.NewMsg(subject)
	m.Data = msg
	if len(headers) > 0 {
		m.Header = headers[0]
	}

	if err := c.conn.PublishMsg(m); err != nil {
		return fmt.Errorf("failed to publish to subject %q: %w", subject, err)
	}

	return nil
}

func (c *conn) RequestVolatile(subject string, msg []byte, timeout time.Duration, headers ...nats.Header) ([]byte, nats.Header, error) {
	m := nats.NewMsg(subject)
	m.Data = msg
	if len(headers) > 0 {
		m.Header = headers[0]
	}

	response, err := c.conn.RequestMsg(m, timeout)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to request on subject %q: %w", subject, err)
	}

	return response.Data, response.Header, nil
}

func (c *conn) PublishVolatileBatch(messages []struct {
	Subject string
	Data    []byte
	Headers nats.Header
}) error {
	for _, msg := range messages {
		m := nats.NewMsg(msg.Subject)
		m.Data = msg.Data
		m.Header = msg.Headers

		if err := c.conn.PublishMsg(m); err != nil {
			return fmt.Errorf("failed to publish batch message to subject %q: %w", msg.Subject, err)
		}
	}

	// Flush to ensure all messages are sent
	if err := c.conn.Flush(); err != nil {
		return fmt.Errorf("failed to flush batch messages: %w", err)
	}

	return nil
}

func (c *conn) FlushTimeout(timeout time.Duration) error {
	return c.conn.FlushTimeout(timeout)
}
