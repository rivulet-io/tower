package mesh

import (
	"fmt"
	"io"
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

// WrapConn defines the interface for all connection operations
type WrapConn interface {
	// Connection management
	Close()
	SetLogCallback(cb func(*NATSLog))

	// Core messaging operations
	SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error)
	SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error)
	PublishVolatile(subject string, msg []byte, headers ...nats.Header) error
	RequestVolatile(subject string, msg []byte, timeout time.Duration, headers ...nats.Header) ([]byte, nats.Header, error)
	PublishVolatileBatch(messages []struct {
		Subject string
		Data    []byte
		Headers nats.Header
	}) error
	FlushTimeout(timeout time.Duration) error

	// Stream operations
	CreateOrUpdateStream(cfg *PersistentConfig) error
	SubscribeStreamViaDurable(subscriberID string, subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error)
	PullPersistentViaDurable(subscriberID string, subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error)
	SubscribePersistentViaEphemeral(subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error)
	PullPersistentViaEphemeral(subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error)
	PublishPersistent(subject string, msg []byte, opts ...nats.PubOpt) error
	PublishPersistentWithOptions(subject string, msg []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	DeleteStream(streamName string) error
	GetStreamInfo(streamName string) (*nats.StreamInfo, error)

	// KV Store operations
	CreateKeyValueStore(cluster string, config KeyValueStoreConfig) error
	GetFromKeyValueStore(bucket, key string) ([]byte, uint64, error)
	PutToKeyValueStore(bucket, key string, value []byte) (uint64, error)
	UpdateToKeyValueStore(bucket, key string, value []byte, expectedRevision uint64) (uint64, error)
	DeleteFromKeyValueStore(bucket, key string) error
	PurgeKeyValueStore(bucket, key string) error
	DeleteKeyValueStore(bucket string) error
	KeyValueStoreExists(bucket string) bool
	ListKeysInKeyValueStore(bucket string) ([]string, error)
	WatchKeyValueStore(bucket, key string) (nats.KeyWatcher, error)
	WatchAllKeysInKeyValueStore(bucket string) (nats.KeyWatcher, error)

	// Object Store operations
	CreateObjectStore(cluster string, config ObjectStoreConfig) error
	GetFromObjectStore(bucket, key string) ([]byte, error)
	PutToObjectStore(bucket, key string, data []byte, metadata map[string]string) error
	DeleteFromObjectStore(bucket, key string) error
	PutToObjectStoreStream(bucket, key string, reader io.Reader, metadata map[string]string) error
	GetFromObjectStoreStream(bucket, key string) (io.ReadCloser, error)
	GetObjectInfo(bucket, key string) (*nats.ObjectInfo, error)
	ListObjects(bucket string) ([]*nats.ObjectInfo, error)
	ObjectExists(bucket, key string) (bool, error)
	DeleteObjectStore(bucket string) error
	PutToObjectStoreChunked(bucket, key string, reader io.Reader, chunkSize int64, metadata map[string]string) error
	CopyObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error

	// Advisory operations
	SubscribeLeaderChange(stream string, handler func(stream string, leader string, myName string), errHandler func(error)) (cancel func(), err error)
}
