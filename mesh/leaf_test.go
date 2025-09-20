package mesh

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// SetupLeafTestThreeNodeCluster creates and returns three interconnected cluster nodes for leaf testing
// This function is based on SetupThreeNodeCluster from cluster_test.go
func SetupLeafTestThreeNodeCluster(t *testing.T) (*Cluster, *Cluster, *Cluster) {
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

// CleanupLeafTestClusters safely closes multiple clusters
func CleanupLeafTestClusters(clusters ...*Cluster) {
	for _, cluster := range clusters {
		if cluster != nil {
			cluster.Close()
		}
	}
}

// Test function to verify the three node cluster setup works
func TestThreeNodeClusterSetup(t *testing.T) {
	t.Run("create three node cluster", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Verify all clusters are running
		if !cluster1.nc.server.Running() {
			t.Error("cluster1 is not running")
		}
		if !cluster2.nc.server.Running() {
			t.Error("cluster2 is not running")
		}
		if !cluster3.nc.server.Running() {
			t.Error("cluster3 is not running")
		}

		// Verify JetStream is enabled on all nodes
		if cluster1.nc.js == nil {
			t.Error("JetStream is not available on cluster1")
		}
		if cluster2.nc.js == nil {
			t.Error("JetStream is not available on cluster2")
		}
		if cluster3.nc.js == nil {
			t.Error("JetStream is not available on cluster3")
		}

		t.Log("✓ Successfully created and verified three node cluster")
	})
}
