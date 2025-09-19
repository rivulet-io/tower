package mesh

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

// ClusterTestConfig holds configuration for test cluster setup
type ClusterTestConfig struct {
	NodeName    string
	NodePort    int
	ClusterPort int
	HTTPPort    int
	StoreDir    string
	Routes      []string
	ClusterName string
	MaxMemory   size.Size
	MaxStorage  size.Size
}

// DefaultClusterTestConfig creates a default test configuration
func DefaultClusterTestConfig(nodeName string, nodeIndex int) *ClusterTestConfig {
	baseNodePort := 4222
	baseClusterPort := 14248
	baseHTTPPort := 18221

	return &ClusterTestConfig{
		NodeName:    nodeName,
		NodePort:    baseNodePort + nodeIndex,
		ClusterPort: baseClusterPort + nodeIndex,
		HTTPPort:    baseHTTPPort + nodeIndex,
		ClusterName: "test-cluster",
		MaxMemory:   size.NewSizeFromMegabytes(50),
		MaxStorage:  size.NewSizeFromMegabytes(100),
	}
}

// WithRoutes sets the routes for the cluster configuration
func (c *ClusterTestConfig) WithRoutes(routes ...string) *ClusterTestConfig {
	c.Routes = routes
	return c
}

// WithStoreDir sets the store directory for the cluster configuration
func (c *ClusterTestConfig) WithStoreDir(dir string) *ClusterTestConfig {
	c.StoreDir = dir
	return c
}

// CreateCluster creates a cluster from the test configuration
func (c *ClusterTestConfig) CreateCluster() (*Cluster, error) {
	opts := NewClusterOptions(c.NodeName).
		WithListen("127.0.0.1", c.NodePort).
		WithStoreDir(c.StoreDir).
		WithClusterName(c.ClusterName).
		WithClusterListen("127.0.0.1", c.ClusterPort).
		WithRoutes(c.Routes).
		WithJetStreamMaxMemory(c.MaxMemory).
		WithJetStreamMaxStore(c.MaxStorage).
		WithHTTPPort(c.HTTPPort)

	return NewCluster(opts)
}

// Helper function to wait for cluster to be ready
func waitForClusterReady(t *testing.T, cluster *Cluster, timeout time.Duration) {
	t.Helper()

	start := time.Now()
	for {
		if time.Since(start) > timeout {
			t.Fatalf("cluster not ready within timeout %v", timeout)
		}

		if cluster.nc.server.Running() && cluster.nc.conn != nil {
			// Test a simple ping to ensure connection is working
			if err := cluster.nc.conn.Flush(); err == nil {
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// Helper function to wait for JetStream to be ready in cluster
func waitForJetStreamReady(t *testing.T, cluster *Cluster, timeout time.Duration) {
	t.Helper()

	start := time.Now()
	for {
		if time.Since(start) > timeout {
			t.Fatalf("JetStream not ready within timeout %v", timeout)
		}

		if cluster.nc.js != nil {
			// Try a simple JetStream operation to check if it's ready
			_, err := cluster.nc.js.AccountInfo()
			if err == nil {
				return
			}
			t.Logf("JetStream not ready yet, error: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

// SetupThreeNodeCluster creates and returns three interconnected cluster nodes
func SetupThreeNodeCluster(t *testing.T) (*Cluster, *Cluster, *Cluster) {
	t.Helper()

	// Create temporary directories for each node
	baseDir := t.TempDir()
	storeDir1 := filepath.Join(baseDir, "node1")
	storeDir2 := filepath.Join(baseDir, "node2")
	storeDir3 := filepath.Join(baseDir, "node3")

	// Create configurations
	config1 := DefaultClusterTestConfig("node1", 0).
		WithStoreDir(storeDir1).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Self-route for JetStream

	config2 := DefaultClusterTestConfig("node2", 1).
		WithStoreDir(storeDir2).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Route to node1

	config3 := DefaultClusterTestConfig("node3", 2).
		WithStoreDir(storeDir3).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Route to node1

	// Create clusters
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

	// Wait for all clusters to be ready
	waitForClusterReady(t, cluster1, 10*time.Second)
	waitForClusterReady(t, cluster2, 10*time.Second)
	waitForClusterReady(t, cluster3, 10*time.Second)

	// Wait for JetStream to be ready on all nodes
	waitForJetStreamReady(t, cluster1, 15*time.Second)
	waitForJetStreamReady(t, cluster2, 15*time.Second)
	waitForJetStreamReady(t, cluster3, 15*time.Second)

	// Additional sleep to ensure cluster formation is complete
	time.Sleep(2 * time.Second)

	return cluster1, cluster2, cluster3
}

// CleanupClusters safely closes multiple clusters
func CleanupClusters(clusters ...*Cluster) {
	for _, cluster := range clusters {
		if cluster != nil {
			cluster.Close()
		}
	}
}

func TestThreeNodeCluster(t *testing.T) {
	t.Run("three node cluster formation", func(t *testing.T) {
		// Use the abstracted setup function
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		t.Log("Check cluster nodes are ready...")

		// Verify all nodes are running
		if !cluster1.nc.server.Running() {
			t.Error("cluster node 1 is not running")
		}
		if !cluster2.nc.server.Running() {
			t.Error("cluster node 2 is not running")
		}
		if !cluster3.nc.server.Running() {
			t.Error("cluster node 3 is not running")
		}

		// Verify cluster connectivity
		clusters := []*Cluster{cluster1, cluster2, cluster3}
		for i, cluster := range clusters {
			if cluster.nc.conn == nil {
				t.Errorf("cluster node %d has nil connection", i+1)
			}
			if cluster.nc.js == nil {
				t.Errorf("cluster node %d has nil JetStream context", i+1)
			}
		}

		t.Logf("Successfully created 3-node cluster with abstracted setup")
	})
}

// Test individual cluster creation with custom configuration
func TestCustomClusterConfiguration(t *testing.T) {
	t.Run("custom cluster config", func(t *testing.T) {
		config := DefaultClusterTestConfig("custom-node", 0).
			WithStoreDir(t.TempDir()).
			WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248))

		cluster, err := config.CreateCluster()
		if err != nil {
			t.Fatalf("failed to create custom cluster: %v", err)
		}
		defer cluster.Close()

		if !cluster.nc.server.Running() {
			t.Error("custom cluster is not running")
		}

		t.Log("Custom cluster configuration test passed")
	})
}
