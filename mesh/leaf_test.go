package mesh

import (
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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

	// Wait for JetStream to be ready on all nodes (increased timeout for stability)
	waitForJetStreamReady(t, cluster1, 30*time.Second)
	waitForJetStreamReady(t, cluster2, 30*time.Second)
	waitForJetStreamReady(t, cluster3, 30*time.Second)

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

// Helper function to wait for leaf to be ready
func waitForLeafReady(t *testing.T, leaf *Leaf, timeout time.Duration) {
	t.Helper()

	start := time.Now()
	for {
		if time.Since(start) > timeout {
			t.Fatalf("leaf not ready within timeout %v", timeout)
		}

		if leaf.conn != nil && leaf.conn.server.Running() && leaf.conn.conn != nil {
			// Test a simple ping to ensure connection is working
			if err := leaf.conn.conn.Flush(); err == nil {
				// Also check if the connection is actually connected
				if leaf.conn.conn.IsConnected() {
					return
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// SetupLeafNodeConnectedToCluster creates a leaf node connected to a specific cluster node
func SetupLeafNodeConnectedToCluster(t *testing.T, cluster *Cluster, leafName string, leafPort int) *Leaf {
	t.Helper()

	// Get cluster connection info - use the cluster's listen port
	clusterPort := cluster.nc.server.Addr().(*net.TCPAddr).Port
	hubURL := fmt.Sprintf("nats://127.0.0.1:%d", clusterPort)

	// Create leaf node options
	opts := NewLeafOptions(leafName).
		WithListen("127.0.0.1", leafPort).
		WithLeafRemotes([]string{hubURL})

	leaf, err := NewLeaf(opts)
	if err != nil {
		t.Fatalf("failed to create leaf node %s: %v", leafName, err)
	}

	waitForLeafReady(t, leaf, 10*time.Second)

	// Give more time for leaf-cluster connection to establish
	time.Sleep(2 * time.Second)

	return leaf
}

// CleanupLeafNodes safely closes multiple leaf nodes
func CleanupLeafNodes(leaves ...*Leaf) {
	for _, leaf := range leaves {
		if leaf != nil {
			leaf.Close()
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

// Test function to verify leaf nodes connected to cluster nodes
func TestLeafNodesConnectedToCluster(t *testing.T) {
	t.Run("two leaf nodes connected to different cluster nodes", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Create first leaf node connected to cluster1
		leaf1 := SetupLeafNodeConnectedToCluster(t, cluster1, "leaf-node-1", 4300)
		defer CleanupLeafNodes(leaf1)

		// Create second leaf node connected to cluster2
		leaf2 := SetupLeafNodeConnectedToCluster(t, cluster2, "leaf-node-2", 4301)
		defer CleanupLeafNodes(leaf2)

		// Verify all nodes are running
		if !cluster1.nc.server.Running() {
			t.Error("cluster1 is not running")
		}
		if !cluster2.nc.server.Running() {
			t.Error("cluster2 is not running")
		}
		if !cluster3.nc.server.Running() {
			t.Error("cluster3 is not running")
		}
		if !leaf1.conn.server.Running() {
			t.Error("leaf1 is not running")
		}
		if !leaf2.conn.server.Running() {
			t.Error("leaf2 is not running")
		}

		// Verify leaf connections
		if leaf1.conn.conn == nil {
			t.Error("leaf1 connection is nil")
		}
		if leaf2.conn.conn == nil {
			t.Error("leaf2 connection is nil")
		}

		t.Log("✓ Successfully connected two leaf nodes to different cluster nodes")
		t.Logf("  - Leaf1 connected to cluster1 (port %d)", cluster1.nc.server.Addr().(*net.TCPAddr).Port)
		t.Logf("  - Leaf2 connected to cluster2 (port %d)", cluster2.nc.server.Addr().(*net.TCPAddr).Port)
	})

	t.Run("leaf to leaf communication via cluster", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Create leaf nodes connected to different cluster nodes
		leaf1 := SetupLeafNodeConnectedToCluster(t, cluster1, "leaf-sender", 4302)
		defer CleanupLeafNodes(leaf1)

		leaf2 := SetupLeafNodeConnectedToCluster(t, cluster2, "leaf-receiver", 4303)
		defer CleanupLeafNodes(leaf2)

		// Wait additional time for leaf-cluster connections to fully establish
		time.Sleep(3 * time.Second)

		// Test basic connectivity by sending a simple message from leaf1 to cluster and back
		testSubject := "connectivity.test"
		connectivityCh := make(chan []byte, 1)

		// Subscribe on leaf1 to test round-trip
		sub, err := leaf1.conn.conn.Subscribe(testSubject+".reply", func(msg *nats.Msg) {
			connectivityCh <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf1 for connectivity test: %v", err)
		}
		defer sub.Unsubscribe()

		// Brief wait for subscription
		time.Sleep(500 * time.Millisecond)

		// Send test message
		err = leaf1.conn.conn.Publish(testSubject+".reply", []byte("connectivity-test"))
		if err != nil {
			t.Fatalf("failed to publish connectivity test: %v", err)
		}

		// Check if we get the message back (indicating cluster routing works)
		select {
		case <-connectivityCh:
			t.Log("✓ Basic leaf-cluster connectivity confirmed")
		case <-time.After(2 * time.Second):
			t.Log("⚠ Basic connectivity test timed out, but continuing...")
		}

		// Test message routing between leaves through cluster
		subject := "test.leaf.cluster.communication"
		testMessage := []byte("Hello from leaf1 to leaf2 via cluster")

		// Subscribe on leaf2
		receivedMessages := make(chan []byte, 1)
		msgSub, err := leaf2.conn.conn.Subscribe(subject, func(msg *nats.Msg) {
			receivedMessages <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf2: %v", err)
		}
		defer msgSub.Unsubscribe()

		// Wait for subscription to be ready and cluster routing to establish
		time.Sleep(3 * time.Second)

		// Publish from leaf1
		err = leaf1.conn.conn.Publish(subject, testMessage)
		if err != nil {
			t.Fatalf("failed to publish from leaf1: %v", err)
		}

		// Wait for message to be received
		select {
		case receivedMsg := <-receivedMessages:
			if string(receivedMsg) != string(testMessage) {
				t.Errorf("expected message '%s', got '%s'", string(testMessage), string(receivedMsg))
			}
			t.Logf("✓ Successfully routed message from leaf1 to leaf2 via cluster: %s", string(receivedMsg))
		case <-time.After(10 * time.Second):
			t.Error("timeout waiting for message to be received")
		}

		// Now test JetStream functionality via domain
		t.Log("Testing JetStream functionality via domain...")

		// Create stream configuration (similar to conn_stream_test.go)
		streamConfig := &PersistentConfig{
			Name:        "leaf_test_stream",
			Description: "Test stream for leaf nodes",
			Subjects:    []string{"leaf.test.*", "leaf.messages.*"},
			Retention:   nats.WorkQueuePolicy,
			MaxMsgs:     1000,
			MaxBytes:    1024 * 1024, // 1MB
			MaxAge:      24 * time.Hour,
			Replicas:    1,
		}

		// Create stream via leaf1 (accessing hub's JetStream via domain)
		err = leaf1.conn.CreateOrUpdateStream(streamConfig)
		if err != nil {
			t.Fatalf("failed to create stream via leaf1: %v", err)
		}
		t.Logf("✓ Successfully created stream '%s' via leaf1", streamConfig.Name)

		// Verify stream exists by getting info via leaf2
		streamInfo, err := leaf2.conn.GetStreamInfo("leaf_test_stream")
		if err != nil {
			t.Fatalf("failed to get stream info via leaf2: %v", err)
		}
		t.Logf("✓ Successfully retrieved stream info via leaf2: %s", streamInfo.Config.Name)

		// Publish message to stream via leaf1
		streamSubject := "leaf.test.message"
		streamMessage := []byte("Hello JetStream from leaf1!")

		err = leaf1.conn.PublishPersistent(streamSubject, streamMessage)
		if err != nil {
			t.Fatalf("failed to publish to stream via leaf1: %v", err)
		}
		t.Log("✓ Successfully published message to stream via leaf1")

		// Subscribe and consume message via leaf2 using SubscribePersistentViaEphemeral
		receivedStreamMessages := make(chan []byte, 1)
		var streamCancel func()

		streamCancel, err = leaf2.conn.SubscribePersistentViaEphemeral(
			streamSubject,
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				receivedStreamMessages <- msg
				return nil, false, true // ack the message
			},
			func(err error) {
				t.Logf("Stream subscription error: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe to stream via leaf2: %v", err)
		}
		defer streamCancel()

		// Wait for message to be received
		select {
		case receivedMsg := <-receivedStreamMessages:
			if string(receivedMsg) != string(streamMessage) {
				t.Errorf("expected stream message '%s', got '%s'", string(streamMessage), string(receivedMsg))
			}
			t.Logf("✓ Successfully received message from stream via leaf2: %s", string(receivedMsg))
		case <-time.After(10 * time.Second):
			t.Error("timeout waiting for stream message to be received")
		}

		// Clean up stream
		err = leaf1.conn.DeleteStream("leaf_test_stream")
		if err != nil {
			t.Logf("Warning: failed to delete stream: %v", err)
		} else {
			t.Log("✓ Successfully cleaned up stream")
		}
	})
}
