package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/rivulet-io/tower/util/size"
)

type ClusterOptions struct {
	serverName               string
	maxPayload               size.Size
	storeDir                 string
	clusterName              string
	clusterListenHost        string
	clusterListenPort        int
	clusterUsername          string
	clusterPassword          string
	jetstreamMaxMemory       size.Size
	jetstreamMaxStore        size.Size
	jetstreamMaxBufferedMsgs int
	jetstreamMaxBufferedSize size.Size
	jetstreamSyncInterval    time.Duration
	gatewayName              string
	gatewayListenHost        string
	gatewayListenPort        int
	gatewayRemotes           *RemoteGateways
	leafListenHost           string
	leafListenPort           int
	leafUsername             string
	leafPassword             string
	routes                   []string
	httpPort                 int
}

func NewClusterOptions(name string) *ClusterOptions {
	return &ClusterOptions{
		serverName: name,
	}
}

func (opt *ClusterOptions) WithMaxPayload(maxPayload size.Size) *ClusterOptions {
	opt.maxPayload = maxPayload
	return opt
}

func DefaultClusterOptions() *ClusterOptions {
	return &ClusterOptions{
		serverName:               "nats-1",
		maxPayload:               size.NewSizeFromMegabytes(64),
		storeDir:                 "/data/nats/jetstream/node-1",
		clusterName:              "PROD_CLUSTER",
		clusterListenHost:        "0.0.0.0",
		clusterListenPort:        6222,
		clusterUsername:          "clusterUser",
		clusterPassword:          "clusterPass",
		jetstreamMaxMemory:       size.NewSizeFromGigabytes(4),
		jetstreamMaxStore:        size.NewSizeFromGigabytes(10),
		jetstreamMaxBufferedMsgs: 1000,
		jetstreamMaxBufferedSize: size.NewSizeFromMegabytes(64),
		jetstreamSyncInterval:    5 * time.Second,
		leafListenHost:           "0.0.0.0",
		leafListenPort:           7422,
		leafUsername:             "leafUser",
		leafPassword:             "leafPass",
		routes: []string{
			"nats-route://nats-2:6222",
			"nats-route://nats-3:6222",
		},
		httpPort: 8222,
	}
}

func (opt *ClusterOptions) toNATSConfig() server.Options {
	return server.Options{
		DontListen: true,
		ServerName: opt.serverName,
		MaxPayload: int32(opt.maxPayload.Bytes()),
		JetStream:  true,
		StoreDir:   opt.storeDir,
		LeafNode: server.LeafNodeOpts{
			Host:     opt.leafListenHost,
			Port:     opt.leafListenPort,
			Username: opt.leafUsername,
			Password: opt.leafPassword,
		},
		Cluster: server.ClusterOpts{
			Name:         opt.clusterName,
			Host:         opt.clusterListenHost,
			Port:         opt.clusterListenPort,
			Username:     opt.clusterUsername,
			Password:     opt.clusterPassword,
			NoAdvertise:  false,
			PingInterval: 30 * time.Second,
		},
		Routes:                strsToURLs(opt.routes),
		JetStreamMaxMemory:    int64(opt.jetstreamMaxMemory.Bytes()),
		JetStreamMaxStore:     int64(opt.jetstreamMaxStore.Bytes()),
		StreamMaxBufferedMsgs: opt.jetstreamMaxBufferedMsgs,
		StreamMaxBufferedSize: int64(opt.jetstreamMaxBufferedSize.Bytes()),
		SyncInterval:          opt.jetstreamSyncInterval,
		Gateway: server.GatewayOpts{
			Name:     opt.gatewayName,
			Host:     opt.gatewayListenHost,
			Port:     opt.gatewayListenPort,
			Gateways: opt.gatewayRemotes.toNATSConfig(),
		},
		HTTPPort: opt.httpPort,
	}
}

type Cluster struct {
	nc *conn
}

func NewCluster(opt *ClusterOptions) (*Cluster, error) {
	so := opt.toNATSConfig()
	nc, err := newConn(&so)
	if err != nil {
		return nil, fmt.Errorf("failed to create nats connection: %w", err)
	}

	return &Cluster{
		nc: nc,
	}, nil
}

func (c *Cluster) Close() {
	c.nc.Close()
}
