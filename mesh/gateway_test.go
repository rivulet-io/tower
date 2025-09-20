package mesh

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rivulet-io/tower/util/size"
)

// GatewayTestConfig holds configuration for gateway test cluster setup
type GatewayTestConfig struct {
	ServerName     string // NATS server name (unique)
	ClusterName    string // NATS cluster name (same for nodes in same cluster)
	NodePort       int
	ClusterPort    int
	HTTPPort       int
	GatewayName    string
	GatewayPort    int
	StoreDir       string
	MaxMemory      size.Size
	MaxStorage     size.Size
	RemoteGateways *RemoteGateways
	Routes         []string
}

// DefaultGatewayTestConfig creates a default gateway test configuration
func DefaultGatewayTestConfig(clusterName, gatewayName string, clusterIndex int) *GatewayTestConfig {
	baseNodePort := 5222 // Different from regular cluster tests
	baseClusterPort := 15248
	baseHTTPPort := 19221
	baseGatewayPort := 7222

	return &GatewayTestConfig{
		ServerName:  clusterName,
		ClusterName: clusterName, // cluster name == gateway name
		NodePort:    baseNodePort + clusterIndex,
		ClusterPort: baseClusterPort + clusterIndex,
		HTTPPort:    baseHTTPPort + clusterIndex,
		GatewayName: gatewayName,
		GatewayPort: baseGatewayPort + clusterIndex,
		MaxMemory:   size.NewSizeFromMegabytes(50),
		MaxStorage:  size.NewSizeFromMegabytes(100),
	}
}

// WithStoreDir sets the store directory for the gateway configuration
func (c *GatewayTestConfig) WithStoreDir(dir string) *GatewayTestConfig {
	c.StoreDir = dir
	return c
}

// WithRemoteGateways sets the remote gateways for the configuration
func (c *GatewayTestConfig) WithRemoteGateways(remotes *RemoteGateways) *GatewayTestConfig {
	c.RemoteGateways = remotes
	return c
}

// WithRoutes sets the routes for the gateway configuration
func (c *GatewayTestConfig) WithRoutes(routes ...string) *GatewayTestConfig {
	c.Routes = routes
	return c
}

// CreateCluster creates a cluster from the gateway test configuration (like cluster_test.go)
func (c *GatewayTestConfig) CreateCluster() (*Cluster, error) {
	opts := NewClusterOptions(c.ServerName). // Server name (unique)
							WithListen("127.0.0.1", c.NodePort).
							WithStoreDir(c.StoreDir).
							WithClusterName(c.ClusterName). // Use cluster-specific name
							WithClusterListen("127.0.0.1", c.ClusterPort).
							WithRoutes(c.Routes).
							WithJetStreamMaxMemory(c.MaxMemory).
							WithJetStreamMaxStore(c.MaxStorage).
							WithHTTPPort(c.HTTPPort) // Only add gateway if GatewayName is not empty
	if c.GatewayName != "" {
		opts = opts.WithGateway(
			c.GatewayName,
			"127.0.0.1",
			c.GatewayPort,
			c.RemoteGateways,
		)
	}

	return NewCluster(opts)
}

// SetupGatewayTestThreeNodeCluster creates a three-node cluster with one gateway node
func SetupGatewayTestThreeNodeCluster(t *testing.T, clusterName string, clusterIndex int, remoteGateways *RemoteGateways) (*Cluster, *Cluster, *Cluster) {
	t.Helper()

	// Create temporary directories for each node (like cluster_test.go)
	baseDir := t.TempDir()
	storeDir1 := filepath.Join(baseDir, "node1")
	storeDir2 := filepath.Join(baseDir, "node2")
	storeDir3 := filepath.Join(baseDir, "node3")

	// Calculate base ports for this cluster
	baseNodePort := 5222 + (clusterIndex * 10)
	baseClusterPort := 15248 + (clusterIndex * 10)
	baseHTTPPort := 19221 + (clusterIndex * 10)
	baseGatewayPort := 7222 + clusterIndex

	// Create configurations for three nodes (like cluster_test.go)
	// Node 1 will be the gateway node
	config1 := &GatewayTestConfig{
		ServerName:     fmt.Sprintf("%s-node1", clusterName), // Unique server name
		ClusterName:    clusterName,                          // Use cluster-specific name
		NodePort:       baseNodePort,
		ClusterPort:    baseClusterPort,
		HTTPPort:       baseHTTPPort,
		GatewayName:    clusterName, // cluster name == gateway name
		GatewayPort:    baseGatewayPort,
		StoreDir:       storeDir1,
		MaxMemory:      size.NewSizeFromMegabytes(50),
		MaxStorage:     size.NewSizeFromMegabytes(100),
		RemoteGateways: remoteGateways,
		Routes:         []string{fmt.Sprintf("nats://127.0.0.1:%d", baseClusterPort)}, // Self-route
	}

	// Node 2 - regular cluster node (no gateway)
	config2 := &GatewayTestConfig{
		ServerName:     fmt.Sprintf("%s-node2", clusterName), // Unique server name
		ClusterName:    clusterName,                          // Use cluster-specific name
		NodePort:       baseNodePort + 1,
		ClusterPort:    baseClusterPort + 1,
		HTTPPort:       baseHTTPPort + 1,
		GatewayName:    "", // No gateway
		GatewayPort:    0,
		StoreDir:       storeDir2,
		MaxMemory:      size.NewSizeFromMegabytes(50),
		MaxStorage:     size.NewSizeFromMegabytes(100),
		RemoteGateways: NewRemoteGateways(),                                           // Empty
		Routes:         []string{fmt.Sprintf("nats://127.0.0.1:%d", baseClusterPort)}, // Route to node1
	}

	// Node 3 - regular cluster node (no gateway)
	config3 := &GatewayTestConfig{
		ServerName:     fmt.Sprintf("%s-node3", clusterName), // Unique server name
		ClusterName:    clusterName,                          // Use cluster-specific name
		NodePort:       baseNodePort + 2,
		ClusterPort:    baseClusterPort + 2,
		HTTPPort:       baseHTTPPort + 2,
		GatewayName:    "", // No gateway
		GatewayPort:    0,
		StoreDir:       storeDir3,
		MaxMemory:      size.NewSizeFromMegabytes(50),
		MaxStorage:     size.NewSizeFromMegabytes(100),
		RemoteGateways: NewRemoteGateways(),                                           // Empty
		Routes:         []string{fmt.Sprintf("nats://127.0.0.1:%d", baseClusterPort)}, // Route to node1
	}

	// Create clusters using the same pattern as cluster_test.go
	cluster1, err := config1.CreateCluster()
	if err != nil {
		t.Fatalf("failed to create cluster node 1: %v", err)
	}

	cluster2, err := config2.CreateCluster()
	if err != nil {
		cluster1.Close()
		t.Fatalf("failed to create cluster node 2: %v", err)
	}

	cluster3, err := config3.CreateCluster()
	if err != nil {
		cluster1.Close()
		cluster2.Close()
		t.Fatalf("failed to create cluster node 3: %v", err)
	}

	// Wait for all clusters to be ready (same as cluster_test.go)
	waitForClusterReady(t, cluster1, 10*time.Second)
	waitForClusterReady(t, cluster2, 10*time.Second)
	waitForClusterReady(t, cluster3, 10*time.Second)

	// Wait for JetStream to be ready on all nodes (same as cluster_test.go)
	waitForJetStreamReady(t, cluster1, 15*time.Second)
	waitForJetStreamReady(t, cluster2, 15*time.Second)
	waitForJetStreamReady(t, cluster3, 15*time.Second)

	// Additional sleep to ensure cluster formation is complete (same as cluster_test.go)
	time.Sleep(2 * time.Second)

	return cluster1, cluster2, cluster3
}

// SetupGatewayTestTwoClusters creates two three-node clusters connected via gateways
func SetupGatewayTestTwoClusters(t *testing.T) (*Cluster, *Cluster, *Cluster, *Cluster, *Cluster, *Cluster) {
	t.Helper()

	// Create remote gateway configurations
	// Cluster A will connect to Cluster B's gateway
	remoteGatewaysA := NewRemoteGateways().
		Add("cluster-b", "nats://127.0.0.1:7223")

	// Cluster B will connect to Cluster A's gateway
	remoteGatewaysB := NewRemoteGateways().
		Add("cluster-a", "nats://127.0.0.1:7222")

	// Create cluster A (three nodes, node1 has gateway)
	clusterA1, clusterA2, clusterA3 := SetupGatewayTestThreeNodeCluster(t, "cluster-a", 0, remoteGatewaysA)

	// Create cluster B (three nodes, node1 has gateway)
	clusterB1, clusterB2, clusterB3 := SetupGatewayTestThreeNodeCluster(t, "cluster-b", 1, remoteGatewaysB)

	// Additional sleep for gateway connections to establish (like cluster_test.go)
	time.Sleep(2 * time.Second)

	return clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3
}

// CleanupGatewayTestClusters safely closes multiple gateway clusters
func CleanupGatewayTestClusters(clusters ...*Cluster) {
	for _, cluster := range clusters {
		if cluster != nil {
			cluster.Close()
		}
	}
}

// Test function to verify gateway cluster setup
func TestGatewayClusterSetup(t *testing.T) {
	t.Run("create three node cluster with gateway", func(t *testing.T) {
		// Create empty remote gateways for single cluster test
		remoteGateways := NewRemoteGateways()

		// Create three node cluster with gateway on node1
		cluster1, cluster2, cluster3 := SetupGatewayTestThreeNodeCluster(t, "test-cluster", 0, remoteGateways)
		defer CleanupGatewayTestClusters(cluster1, cluster2, cluster3)

		// Verify all clusters are running (same checks as cluster_test.go)
		if !cluster1.nc.server.Running() {
			t.Error("cluster node 1 is not running")
		}
		if !cluster2.nc.server.Running() {
			t.Error("cluster node 2 is not running")
		}
		if !cluster3.nc.server.Running() {
			t.Error("cluster node 3 is not running")
		}

		// Verify JetStream is enabled on all nodes
		if cluster1.nc.js == nil {
			t.Error("JetStream is not available on cluster node 1")
		}
		if cluster2.nc.js == nil {
			t.Error("JetStream is not available on cluster node 2")
		}
		if cluster3.nc.js == nil {
			t.Error("JetStream is not available on cluster node 3")
		}

		t.Logf("✓ Successfully created three-node cluster 'test-cluster' with gateway on node1")
		t.Logf("  - Node1 (gateway): port %d", cluster1.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("  - Node2: port %d", cluster2.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("  - Node3: port %d", cluster3.nc.server.Addr().(*net.TCPAddr).Port)
	})
} // Test function to verify two clusters connected via gateways

func TestGatewayTwoClusterConnection(t *testing.T) {
	t.Run("connect two three-node clusters via gateways", func(t *testing.T) {
		clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3 := SetupGatewayTestTwoClusters(t)
		defer CleanupGatewayTestClusters(clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3)

		// Verify all clusters are running (same checks as cluster_test.go)
		clusters := []*Cluster{clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3}
		clusterNames := []string{"cluster A node 1 (gateway)", "cluster A node 2", "cluster A node 3",
			"cluster B node 1 (gateway)", "cluster B node 2", "cluster B node 3"}

		for i, cluster := range clusters {
			if !cluster.nc.server.Running() {
				t.Errorf("%s is not running", clusterNames[i])
			}
			if cluster.nc.js == nil {
				t.Errorf("JetStream is not available on %s", clusterNames[i])
			}
		}

		t.Logf("✓ Successfully created two three-node gateway-connected clusters:")
		t.Logf("  - Cluster A:")
		t.Logf("    - Node1 (gateway): port %d", clusterA1.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("    - Node2: port %d", clusterA2.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("    - Node3: port %d", clusterA3.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("  - Cluster B:")
		t.Logf("    - Node1 (gateway): port %d", clusterB1.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("    - Node2: port %d", clusterB2.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("    - Node3: port %d", clusterB3.nc.server.Addr().(*net.TCPAddr).Port)

		// Test basic cross-cluster connectivity using gateway nodes
		t.Log("Testing basic cross-cluster messaging via gateways...")
		testBasicCrossClusterMessaging(t, clusterA1, clusterB1) // Use gateway nodes
	})
}

// testBasicCrossClusterMessaging tests basic messaging between clusters via gateways
func testBasicCrossClusterMessaging(t *testing.T, clusterA, clusterB *Cluster) {
	t.Helper()

	subject := "gateway.test.message"
	testMessage := []byte("Hello from cluster A to cluster B via gateway!")
	receivedMessages := make(chan []byte, 1)

	// Subscribe on cluster B
	sub, err := clusterB.nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		receivedMessages <- msg.Data
	})
	if err != nil {
		t.Fatalf("failed to subscribe on cluster B: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for subscription to propagate across gateway
	time.Sleep(3 * time.Second)

	// Publish from cluster A
	err = clusterA.nc.conn.Publish(subject, testMessage)
	if err != nil {
		t.Fatalf("failed to publish from cluster A: %v", err)
	}

	// Wait for message to be received
	select {
	case receivedMsg := <-receivedMessages:
		if string(receivedMsg) != string(testMessage) {
			t.Errorf("expected message '%s', got '%s'", string(testMessage), string(receivedMsg))
		}
		t.Logf("✓ Successfully routed message via gateway: %s", string(receivedMsg))
	case <-time.After(10 * time.Second):
		t.Error("timeout waiting for cross-cluster message via gateway")
	}
}
