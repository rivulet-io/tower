package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type conn struct {
	server *server.Server
	conn   *nats.Conn
}

func newConn(opt *server.Options) (*conn, error) {
	srv, err := server.NewServer(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats server: %w", err)
	}

	go srv.Start()

	if !srv.ReadyForConnections(10 * time.Second) {
		return nil, fmt.Errorf("nats server not ready for connections")
	}

	nc, err := nats.Connect(srv.ClientURL(), nats.InProcessServer(srv))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats server: %w", err)
	}

	return &conn{
		server: srv,
		conn:   nc,
	}, nil
}

func (c *conn) Close() {
	c.conn.Close()
	c.server.Shutdown()
	c.server.WaitForShutdown()
}
