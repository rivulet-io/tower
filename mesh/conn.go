package mesh

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var _ server.Logger = (*DebugLogger)(nil)

type DebugLogger struct {
}

// Debugf implements server.Logger.
func (d *DebugLogger) Debugf(format string, v ...any) {
	log.Printf("[DEBUG] "+format, v...)
}

// Errorf implements server.Logger.
func (d *DebugLogger) Errorf(format string, v ...any) {
	log.Printf("[ERROR] "+format, v...)
}

// Fatalf implements server.Logger.
func (d *DebugLogger) Fatalf(format string, v ...any) {
	log.Fatalf("[FATAL] "+format, v...)
}

// Noticef implements server.Logger.
func (d *DebugLogger) Noticef(format string, v ...any) {
	log.Printf("[NOTICE] "+format, v...)
}

// Tracef implements server.Logger.
func (d *DebugLogger) Tracef(format string, v ...any) {
	log.Printf("[TRACE] "+format, v...)
}

// Warnf implements server.Logger.
func (d *DebugLogger) Warnf(format string, v ...any) {
	log.Printf("[WARN] "+format, v...)
}

type conn struct {
	server *server.Server
	conn   *nats.Conn
	js     nats.JetStreamContext
}

var (
	EnableDebugLog    = false
	EnableTraceLog    = false
	EnableSysTraceLog = false
)

func newServerConn(opt *server.Options) (*conn, error) {
	srv, err := server.NewServer(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats server: %w", err)
	}

	if EnableDebugLog || EnableTraceLog || EnableSysTraceLog {
		srv.SetLoggerV2(&DebugLogger{}, EnableDebugLog, EnableTraceLog, EnableSysTraceLog)
	}

	srv.Start()

	if !srv.ReadyForConnections(15 * time.Second) {
		return nil, fmt.Errorf("nats server not ready for connections")
	}

	nc, err := nats.Connect(srv.ClientURL(), nats.InProcessServer(srv))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats server: %w", err)
	}

	js, err := nc.JetStream(nats.Domain(defaultClusterName))
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream context: %w", err)
	}

	return &conn{
		server: srv,
		conn:   nc,
		js:     js,
	}, nil
}

func newClientConn(servers []string, username, password string) (*conn, error) {
	nc, err := nats.Connect(strings.Join(servers, ","),
		nats.UserInfo(username, password),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats server: %w", err)
	}

	js, err := nc.JetStream(nats.Domain(defaultClusterName))
	if err != nil {
		return nil, fmt.Errorf("failed to get jetstream context: %w", err)
	}

	return &conn{
		conn: nc,
		js:   js,
	}, nil
}

func (c *conn) Close() {
	c.conn.Close()
	if c.server != nil {
		c.server.Shutdown()
		c.server.WaitForShutdown()
	}
}
