package mesh

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rivulet-io/tower/util/size"
)

type PersistentConfig struct {
	// Description is an optional description of the stream.
	Description string

	// Subjects is a list of subjects that the stream is listening on.
	// Wildcards are supported. Subjects cannot be set if the stream is
	// created as a mirror.
	Subjects []string

	// Retention defines the message retention policy for the stream.
	// Defaults to LimitsPolicy.
	Retention nats.RetentionPolicy

	// MaxConsumers specifies the maximum number of consumers allowed for
	// the stream.
	MaxConsumers int

	// MaxMsgs is the maximum number of messages the stream will store.
	// After reaching the limit, stream adheres to the discard policy.
	// If not set, server default is -1 (unlimited).
	MaxMsgs int64

	// MaxBytes is the maximum total size of messages the stream will store.
	// After reaching the limit, stream adheres to the discard policy.
	// If not set, server default is -1 (unlimited).
	MaxBytes int64

	// MaxAge is the maximum age of messages that the stream will retain.
	MaxAge time.Duration

	// MaxMsgsPerSubject is the maximum number of messages per subject that
	// the stream will retain.
	MaxMsgsPerSubject int64

	// MaxMsgSize is the maximum size of any single message in the stream.
	MaxMsgSize size.Size

	// Replicas is the number of stream replicas in clustered JetStream.
	// Defaults to 1, maximum is 5.
	Replicas int

	// NoAck is a flag to disable acknowledging messages received by this
	// stream.
	//
	// If set to true, publish methods from the JetStream client will not
	// work as expected, since they rely on acknowledgements. Core NATS
	// publish methods should be used instead. Note that this will make
	// message delivery less reliable.
	NoAck bool

	// Duplicates is the window within which to track duplicate messages.
	// If not set, server default is 2 minutes.
	Duplicates time.Duration

	// Metadata is an optional set of key/value pairs that can be used to
	// store additional information about the stream.
	Metadata map[string]string
}

type PullOptions struct {
	Batch    int
	MaxWait  time.Duration
	Interval time.Duration
}

func (c *conn) CreateOrUpdateStream(cfg *PersistentConfig) error {
	if len(cfg.Subjects) == 0 {
		return fmt.Errorf("subjects cannot be empty")
	}

	// Use first subject as stream name, but sanitize it properly
	streamName := cfg.Subjects[0]
	// Remove wildcards and convert to valid stream name
	streamName = strings.ReplaceAll(streamName, "*", "")
	streamName = strings.ReplaceAll(streamName, ">", "")
	streamName = strings.ReplaceAll(streamName, ".", "_")
	if streamName == "" || streamName == "_" {
		streamName = "default_stream"
	}

	sc := &nats.StreamConfig{
		Name:              streamName,
		Description:       cfg.Description,
		Subjects:          cfg.Subjects,
		Retention:         cfg.Retention,
		Storage:           nats.FileStorage,
		Compression:       nats.S2Compression,
		MaxConsumers:      cfg.MaxConsumers,
		MaxMsgs:           cfg.MaxMsgs,
		MaxBytes:          cfg.MaxBytes,
		MaxAge:            cfg.MaxAge,
		MaxMsgsPerSubject: cfg.MaxMsgsPerSubject,
		MaxMsgSize:        int32(cfg.MaxMsgSize.Bytes()),
		Replicas:          cfg.Replicas,
		NoAck:             cfg.NoAck,
		Duplicates:        cfg.Duplicates,
		Metadata:          cfg.Metadata,
	}

	_, err := c.js.AddStream(sc)
	if err != nil {
		if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			_, err = c.js.UpdateStream(sc)
			if err != nil {
				return fmt.Errorf("failed to update stream: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create stream: %w", err)
		}
	}

	return nil
}

func (c *conn) SubscribeStreamViaDurable(subscriberID string, subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.js.Subscribe(subject, func(msg *nats.Msg) {
		response, ok, ack := handler(msg.Subject, msg.Data)
		if ack {
			if err := msg.Ack(); err != nil {
				errHandler(fmt.Errorf("failed to acknowledge message on subject %q: %w", msg.Subject, err))
			}
		}
		if !ok || msg.Reply == "" {
			return
		}
		if err := msg.Respond(response); err != nil {
			errHandler(fmt.Errorf("failed to respond to message on subject %q: %w", msg.Subject, err))
		}
	}, nats.Durable(subscriberID), nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	return func() {
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from subject %q: %w", subject, err))
		}
	}, nil
}

func (c *conn) PullPersistentViaDurable(subscriberID string, subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.js.PullSubscribe(subject, subscriberID, nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	if option.Batch <= 0 {
		option.Batch = 5
	}
	if option.MaxWait <= 0 {
		option.MaxWait = 5 * time.Second
	}
	if option.Interval <= 0 {
		option.Interval = 100 * time.Millisecond
	}

	cancelFunc := make(chan struct{})
	go func() {
		const maxErrCount = 5
		errCount := 0
		for {
			select {
			case <-cancelFunc:
				return
			default:
				msgs, err := sub.Fetch(option.Batch, nats.MaxWait(option.MaxWait))
				if err != nil && err != nats.ErrTimeout {
					errHandler(fmt.Errorf("failed to fetch messages from subject %q: %w (count=%d)", subject, err, errCount))
					errCount++
					if errCount >= maxErrCount {
						return
					}
					continue
				}
				for _, msg := range msgs {
					response, ok, ack := handler(msg.Subject, msg.Data)
					if ack {
						if err := msg.Ack(); err != nil {
							errHandler(fmt.Errorf("failed to acknowledge message on subject %q: %w", msg.Subject, err))
						}
					}
					if !ok || msg.Reply == "" {
						continue
					}
					if err := msg.Respond(response); err != nil {
						errHandler(fmt.Errorf("failed to respond to message on subject %q: %w", msg.Subject, err))
					}
				}
				// Reset error count on successful fetch
				errCount = 0
			}
			time.Sleep(option.Interval)
		}
	}()
	return func() {
		close(cancelFunc)
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from subject %q: %w", subject, err))
		}
	}, nil
}

func (c *conn) SubscribePersistentViaEphemeral(subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.js.Subscribe(subject, func(msg *nats.Msg) {
		response, ok, ack := handler(msg.Subject, msg.Data)
		if ack {
			if err := msg.Ack(); err != nil {
				errHandler(fmt.Errorf("failed to acknowledge message on subject %q: %w", msg.Subject, err))
			}
		}
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

func (c *conn) PullPersistentViaEphemeral(subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error)) (cancel func(), err error) {
	sub, err := c.js.PullSubscribe(subject, "", nats.ManualAck())
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to subject %q: %w", subject, err)
	}

	if option.Batch <= 0 {
		option.Batch = 5
	}
	if option.MaxWait <= 0 {
		option.MaxWait = 5 * time.Second
	}
	if option.Interval <= 0 {
		option.Interval = 100 * time.Millisecond
	}

	cancelFunc := make(chan struct{})
	go func() {
		const maxErrCount = 5
		errCount := 0
		for {
			select {
			case <-cancelFunc:
				return
			default:
				msgs, err := sub.Fetch(option.Batch, nats.MaxWait(option.MaxWait))
				if err != nil && err != nats.ErrTimeout {
					errHandler(fmt.Errorf("failed to fetch messages from subject %q: %w (count=%d)", subject, err, errCount))
					errCount++
					if errCount >= maxErrCount {
						return
					}
					continue
				}
				for _, msg := range msgs {
					response, ok, ack := handler(msg.Subject, msg.Data)
					if ack {
						if err := msg.Ack(); err != nil {
							errHandler(fmt.Errorf("failed to acknowledge message on subject %q: %w", msg.Subject, err))
						}
					}
					if !ok || msg.Reply == "" {
						continue
					}
					if err := msg.Respond(response); err != nil {
						errHandler(fmt.Errorf("failed to respond to message on subject %q: %w", msg.Subject, err))
					}
				}
				// Reset error count on successful fetch
				errCount = 0
			}
			time.Sleep(option.Interval)
		}
	}()

	return func() {
		close(cancelFunc)
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from subject %q: %w", subject, err))
		}
	}, nil
}

func (c *conn) PublishPersistent(subject string, msg []byte) error {
	_, err := c.js.Publish(subject, msg)
	if err != nil {
		return fmt.Errorf("failed to publish to subject %q: %w", subject, err)
	}

	return nil
}

func (c *conn) PublishPersistentWithOptions(subject string, msg []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	ack, err := c.js.Publish(subject, msg, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to publish to subject %q: %w", subject, err)
	}

	return ack, nil
}

func (c *conn) DeleteStream(streamName string) error {
	err := c.js.DeleteStream(streamName)
	if err != nil {
		return fmt.Errorf("failed to delete stream %q: %w", streamName, err)
	}
	return nil
}

func (c *conn) GetStreamInfo(streamName string) (*nats.StreamInfo, error) {
	info, err := c.js.StreamInfo(streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info for %q: %w", streamName, err)
	}
	return info, nil
}
