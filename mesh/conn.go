package mesh

import (
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var _ server.Logger = (*DebugLogger)(nil)

const (
	NATSLogTypeDebug  = "debug"
	NATSLogTypeError  = "error"
	NATSLogTypeFatal  = "fatal"
	NATSLogTypeNotice = "notice"
	NATSLogTypeTrace  = "trace"
	NATSLogTypeWarn   = "warn"
)

type NATSLog struct {
	Type string `json:"type"`
	Msg  string `json:"msg"`
}

type DebugLogger struct {
	logChan chan *NATSLog
}

// Debugf implements server.Logger.
func (d *DebugLogger) Debugf(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeDebug,
		Msg:  fmt.Sprintf(format, v...),
	}
}

// Errorf implements server.Logger.
func (d *DebugLogger) Errorf(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeError,
		Msg:  fmt.Sprintf(format, v...),
	}
}

// Fatalf implements server.Logger.
func (d *DebugLogger) Fatalf(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeFatal,
		Msg:  fmt.Sprintf(format, v...),
	}
}

// Noticef implements server.Logger.
func (d *DebugLogger) Noticef(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeNotice,
		Msg:  fmt.Sprintf(format, v...),
	}
}

// Tracef implements server.Logger.
func (d *DebugLogger) Tracef(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeTrace,
		Msg:  fmt.Sprintf(format, v...),
	}
}

// Warnf implements server.Logger.
func (d *DebugLogger) Warnf(format string, v ...any) {
	d.logChan <- &NATSLog{
		Type: NATSLogTypeWarn,
		Msg:  fmt.Sprintf(format, v...),
	}
}

type conn struct {
	server   *server.Server
	conn     *nats.Conn
	js       nats.JetStreamContext
	logger   *DebugLogger
	callback func(*NATSLog)
}

func newServerConn(opt *server.Options) (*conn, error) {
	srv, err := server.NewServer(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats server: %w", err)
	}

	dl := &DebugLogger{
		logChan: make(chan *NATSLog, 4096),
	}
	srv.SetLoggerV2(dl, true, true, false)
	srv.ConfigureLogger()

	c := &conn{}

	go func() {
		for log := range dl.logChan {
			if c.callback != nil {
				c.callback(log)
			}
		}
	}()

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

	c.server = srv
	c.conn = nc
	c.js = js
	c.logger = dl

	return c, nil
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

func (c *conn) SetLogCallback(cb func(*NATSLog)) {
	c.callback = cb
}

type WrapConn interface{}
