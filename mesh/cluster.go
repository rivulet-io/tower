package mesh

import (
	"fmt"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/rivulet-io/tower/util/size"
)

const defaultClusterName = "rivulet-cluster"

type ClusterOptions struct {
	serverName               string
	listenHost               string
	listenPort               int
	maxPayload               size.Size
	storeDir                 string
	clusterName              string
	clusterListenHost        string
	clusterListenPort        int
	clusterUsername          string
	clusterPassword          string
	clusterPingInterval      time.Duration
	clusterNoAdvertise       bool
	jetstreamMaxMemory       size.Size
	jetstreamMaxStore        size.Size
	jetstreamMaxBufferedMsgs int
	jetstreamMaxBufferedSize size.Size
	jetstreamSyncInterval    time.Duration
	gatewayName              string
	gatewayUsername          string
	gatewayPassword          string
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
		serverName:          name,
		clusterPingInterval: 10 * time.Second, // 기본값 설정
		clusterNoAdvertise:  false,            // 기본값 설정
	}
}

func (opt *ClusterOptions) WithListen(host string, port int) *ClusterOptions {
	opt.listenHost = host
	opt.listenPort = port
	return opt
}

func (opt *ClusterOptions) WithMaxPayload(maxPayload size.Size) *ClusterOptions {
	opt.maxPayload = maxPayload
	return opt
}

func (opt *ClusterOptions) WithStoreDir(storeDir string) *ClusterOptions {
	opt.storeDir = storeDir
	return opt
}

func (opt *ClusterOptions) WithClusterName(clusterName string) *ClusterOptions {
	opt.clusterName = clusterName
	return opt
}

func (opt *ClusterOptions) WithClusterListen(host string, port int) *ClusterOptions {
	opt.clusterListenHost = host
	opt.clusterListenPort = port
	return opt
}

func (opt *ClusterOptions) WithClusterAuth(username, password string) *ClusterOptions {
	opt.clusterUsername = username
	opt.clusterPassword = password
	return opt
}

func (opt *ClusterOptions) WithClusterPingInterval(interval time.Duration) *ClusterOptions {
	opt.clusterPingInterval = interval
	return opt
}

func (opt *ClusterOptions) WithClusterNoAdvertise(noAdvertise bool) *ClusterOptions {
	opt.clusterNoAdvertise = noAdvertise
	return opt
}

func (opt *ClusterOptions) WithJetStreamMaxMemory(maxMemory size.Size) *ClusterOptions {
	opt.jetstreamMaxMemory = maxMemory
	return opt
}

func (opt *ClusterOptions) WithJetStreamMaxStore(maxStore size.Size) *ClusterOptions {
	opt.jetstreamMaxStore = maxStore
	return opt
}

func (opt *ClusterOptions) WithJetStreamBuffered(maxMsgs int, maxSize size.Size) *ClusterOptions {
	opt.jetstreamMaxBufferedMsgs = maxMsgs
	opt.jetstreamMaxBufferedSize = maxSize
	return opt
}

func (opt *ClusterOptions) WithJetStreamSyncInterval(interval time.Duration) *ClusterOptions {
	opt.jetstreamSyncInterval = interval
	return opt
}

func (opt *ClusterOptions) WithGateway(name, host string, port int, username string, password string, remotes *RemoteGateways) *ClusterOptions {
	opt.gatewayName = name
	opt.gatewayListenHost = host
	opt.gatewayListenPort = port
	opt.gatewayUsername = username
	opt.gatewayPassword = password
	opt.gatewayRemotes = remotes
	return opt
}

func (opt *ClusterOptions) WithLeafNode(host string, port int, username, password string) *ClusterOptions {
	opt.leafListenHost = host
	opt.leafListenPort = port
	opt.leafUsername = username
	opt.leafPassword = password
	return opt
}

func (opt *ClusterOptions) WithRoutes(routes []string) *ClusterOptions {
	opt.routes = routes
	return opt
}

func (opt *ClusterOptions) WithHTTPPort(port int) *ClusterOptions {
	opt.httpPort = port
	return opt
}

func (opt *ClusterOptions) toNATSConfig() server.Options {
	return server.Options{
		ServerName: opt.serverName,
		Host:       opt.listenHost,
		Port:       opt.listenPort,
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
			NoAdvertise:  opt.clusterNoAdvertise,
			PingInterval: opt.clusterPingInterval,
		},
		Routes:                strsToURLs(opt.routes),
		JetStreamMaxMemory:    int64(opt.jetstreamMaxMemory.Bytes()),
		JetStreamMaxStore:     int64(opt.jetstreamMaxStore.Bytes()),
		JetStreamDomain:       defaultClusterName,
		StreamMaxBufferedMsgs: opt.jetstreamMaxBufferedMsgs,
		StreamMaxBufferedSize: int64(opt.jetstreamMaxBufferedSize.Bytes()),
		SyncInterval:          opt.jetstreamSyncInterval,
		Gateway: func() server.GatewayOpts {
			if opt.gatewayName == "" || opt.gatewayRemotes == nil || len(*opt.gatewayRemotes) == 0 {
				return server.GatewayOpts{}
			}

			return server.GatewayOpts{
				Name:     opt.gatewayName,
				Host:     opt.gatewayListenHost,
				Port:     opt.gatewayListenPort,
				Username: opt.gatewayUsername,
				Password: opt.gatewayPassword,
				Gateways: opt.gatewayRemotes.toNATSConfig(),
			}
		}(),
		HTTPPort: opt.httpPort,
	}
}

type Cluster struct {
	nc *conn
}

func NewCluster(opt *ClusterOptions) (*Cluster, error) {
	so := opt.toNATSConfig()
	nc, err := newServerConn(&so)
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
