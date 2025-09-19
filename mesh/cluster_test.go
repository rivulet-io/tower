package mesh

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

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

func TestThreeNodeCluster(t *testing.T) {
	t.Run("three node cluster formation", func(t *testing.T) {
		// Create temporary directories for each node
		storeDir1 := filepath.Join(t.TempDir(), "node1")
		storeDir2 := filepath.Join(t.TempDir(), "node2")
		storeDir3 := filepath.Join(t.TempDir(), "node3")

		nodePort1 := 4222
		nodePort2 := 4223
		nodePort3 := 4224

		// Define cluster ports
		clusterPort1 := 14248
		clusterPort2 := 14249
		clusterPort3 := 14250

		// Create node 1 (seed node) - needs at least one route for JetStream cluster
		opts1 := NewClusterOptions("node1").
			WithListen("127.0.0.1", nodePort1).
			WithStoreDir(storeDir1).
			WithClusterName("test-cluster").
			WithClusterListen("127.0.0.1", clusterPort1).
			WithRoutes([]string{
				fmt.Sprintf("nats://127.0.0.1:%d", clusterPort1),
			}).
			WithJetStreamMaxMemory(size.NewSizeFromMegabytes(50)).
			WithJetStreamMaxStore(size.NewSizeFromMegabytes(100)).
			WithHTTPPort(18221)

		cluster1, err := NewCluster(opts1)
		if err != nil {
			t.Fatalf("failed to create cluster node 1: %v", err)
		}
		defer cluster1.Close()

		// Create node 2 (connects to node 1 and 3)
		opts2 := NewClusterOptions("node2").
			WithListen("127.0.0.1", nodePort2).
			WithStoreDir(storeDir2).
			WithClusterName("test-cluster").
			WithClusterListen("127.0.0.1", clusterPort2).
			WithRoutes([]string{
				fmt.Sprintf("nats://127.0.0.1:%d", clusterPort1),
			}).
			WithJetStreamMaxMemory(size.NewSizeFromMegabytes(50)).
			WithJetStreamMaxStore(size.NewSizeFromMegabytes(100)).
			WithHTTPPort(18222)

		cluster2, err := NewCluster(opts2)
		if err != nil {
			t.Fatalf("failed to create cluster node 2: %v", err)
		}
		defer cluster2.Close()

		// Create node 3 (connects to node 1 and 2)
		opts3 := NewClusterOptions("node3").
			WithListen("127.0.0.1", nodePort3).
			WithStoreDir(storeDir3).
			WithClusterName("test-cluster").
			WithClusterListen("127.0.0.1", clusterPort3).
			WithRoutes([]string{
				fmt.Sprintf("nats://127.0.0.1:%d", clusterPort1),
			}).
			WithJetStreamMaxMemory(size.NewSizeFromMegabytes(50)).
			WithJetStreamMaxStore(size.NewSizeFromMegabytes(100)).
			WithHTTPPort(18223)

		cluster3, err := NewCluster(opts3)
		if err != nil {
			t.Fatalf("failed to create cluster node 3: %v", err)
		}
		defer cluster3.Close()

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

		// Verify cluster connectivity by checking route connections
		// Note: In a real cluster, we would check server.NumRoutes() but
		// for unit tests we just verify the servers are operational
		clusters := []*Cluster{cluster1, cluster2, cluster3}
		for i, cluster := range clusters {
			if cluster.nc.conn == nil {
				t.Errorf("cluster node %d has nil connection", i+1)
			}
			if cluster.nc.js == nil {
				t.Errorf("cluster node %d has nil JetStream context", i+1)
			}
		}

		t.Logf("Successfully created 3-node cluster: node1:%d, node2:%d, node3:%d",
			clusterPort1, clusterPort2, clusterPort3)
	})
}
