package mesh

import (
	"fmt"

	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/jsm.go/api/jetstream/advisory"
	"github.com/nats-io/nats.go"
)

func (c *conn) SubscribeLeaderChange(stream string, handler func(stream string, leader string, myName string), errHandler func(error)) (cancel func(), err error) {
	subject := fmt.Sprintf("$JS.API.STREAM.LEADER.ELECTED.%s", stream)
	myName := c.conn.Opts.Name
	sub, err := c.conn.Subscribe(subject, func(msg *nats.Msg) {
		kind, message, err := api.ParseMessage(msg.Data)
		if err != nil {
			errHandler(fmt.Errorf("failed to parse stream leader change message for stream %q: %w", stream, err))
			return
		}
		switch value := message.(type) {
		case advisory.JSStreamLeaderElectedV1:
			handler(stream, value.Leader, myName)
		default:
			errHandler(fmt.Errorf("unknown stream leader change message type %q for stream %q", kind, stream))
		}
	})
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to stream leader change for stream %q: %w", stream, err)
	}

	return func() {
		if err := sub.Unsubscribe(); err != nil {
			errHandler(fmt.Errorf("failed to unsubscribe from stream leader change for stream %q: %w", stream, err))
		}
	}, nil
}
