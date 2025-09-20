package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/rivulet-io/tower/util/size"
)

type LeafOptions struct {
	serverName               string
	host                     string
	port                     int
	username                 string
	password                 string
	leafRemotes              [][]string
	storeDir                 string
	jetstreamEnabled         bool
	jetstreamMaxMemory       size.Size
	jetstreamMaxStore        size.Size
	jetstreamMaxBufferedMsgs int
	jetstreamMaxBufferedSize size.Size
	jetstreamSyncInterval    time.Duration
}

func NewLeafOptions(name string) *LeafOptions {
	return &LeafOptions{
		serverName: name,
	}
}

func (opt *LeafOptions) WithListen(host string, port int) *LeafOptions {
	opt.host = host
	opt.port = port
	return opt
}

func (opt *LeafOptions) WithLeafAuth(username, password string) *LeafOptions {
	opt.username = username
	opt.password = password
	return opt
}

func (opt *LeafOptions) WithLeafRemotes(remotes ...[]string) *LeafOptions {
	opt.leafRemotes = remotes
	return opt
}

func (opt *LeafOptions) WithStoreDir(dir string) *LeafOptions {
	opt.storeDir = dir
	return opt
}

func (opt *LeafOptions) WithJetStream(enabled bool) *LeafOptions {
	opt.jetstreamEnabled = enabled
	return opt
}

func (opt *LeafOptions) WithJetStreamMaxMemory(maxMemory size.Size) *LeafOptions {
	opt.jetstreamMaxMemory = maxMemory
	return opt
}

func (opt *LeafOptions) WithJetStreamMaxStore(maxStore size.Size) *LeafOptions {
	opt.jetstreamMaxStore = maxStore
	return opt
}

func (opt *LeafOptions) WithJetStreamBuffered(maxMsgs int, maxSize size.Size) *LeafOptions {
	opt.jetstreamMaxBufferedMsgs = maxMsgs
	opt.jetstreamMaxBufferedSize = maxSize
	return opt
}

func (opt *LeafOptions) WithJetStreamSyncInterval(interval time.Duration) *LeafOptions {
	opt.jetstreamSyncInterval = interval
	return opt
}

func (opt *LeafOptions) toNATSConfig() *server.Options {
	leafRemotes := make([]*server.RemoteLeafOpts, 0, len(opt.leafRemotes))
	for _, r := range opt.leafRemotes {
		leafRemotes = append(leafRemotes, &server.RemoteLeafOpts{
			URLs: strsToURLs(r),
		})
	}

	config := &server.Options{
		ServerName: opt.serverName,
		Host:       opt.host,
		Port:       opt.port,
		LeafNode: server.LeafNodeOpts{
			Username: opt.username,
			Password: opt.password,
			Remotes:  leafRemotes,
		},
	}

	// Add JetStream configuration if enabled
	// if opt.jetstreamEnabled {
	// 	config.JetStream = true
	// 	config.StoreDir = opt.storeDir
	// 	config.JetStreamDomain = defaultClusterName

	// 	if opt.jetstreamMaxMemory.Bytes() > 0 {
	// 		config.JetStreamMaxMemory = int64(opt.jetstreamMaxMemory.Bytes())
	// 	}
	// 	if opt.jetstreamMaxStore.Bytes() > 0 {
	// 		config.JetStreamMaxStore = int64(opt.jetstreamMaxStore.Bytes())
	// 	}
	// 	if opt.jetstreamMaxBufferedMsgs > 0 {
	// 		config.StreamMaxBufferedMsgs = opt.jetstreamMaxBufferedMsgs
	// 	}
	// 	if opt.jetstreamMaxBufferedSize.Bytes() > 0 {
	// 		config.StreamMaxBufferedSize = int64(opt.jetstreamMaxBufferedSize.Bytes())
	// 	}
	// 	if opt.jetstreamSyncInterval > 0 {
	// 		config.SyncInterval = opt.jetstreamSyncInterval
	// 	}
	// }

	return config
}

type Leaf struct {
	conn *conn
}

func NewLeaf(opt *LeafOptions) (*Leaf, error) {
	so := opt.toNATSConfig()
	nc, err := newServerConn(so)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats connection: %w", err)
	}

	return &Leaf{
		conn: nc,
	}, nil
}

func (l *Leaf) Close() {
	if l.conn != nil {
		l.conn.Close()
	}
}
