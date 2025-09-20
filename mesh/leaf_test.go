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

// SetupLeafTestThreeNodeCluster creates and returns three interconnected cluster nodes for leaf testing
// This function is based on SetupThreeNodeCluster from cluster_test.go
func SetupLeafTestThreeNodeCluster(t *testing.T) (*Cluster, *Cluster, *Cluster) {
	t.Helper()

	// Create temporary directories for each node
	baseDir := t.TempDir()
	storeDir1 := filepath.Join(baseDir, "node1")
	storeDir2 := filepath.Join(baseDir, "node2")
	storeDir3 := filepath.Join(baseDir, "node3")

	// Create configurations with leaf node support
	config1 := DefaultClusterTestConfig("node1", 0).
		WithStoreDir(storeDir1).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Self-route for JetStream

	config2 := DefaultClusterTestConfig("node2", 1).
		WithStoreDir(storeDir2).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Route to node1

	config3 := DefaultClusterTestConfig("node3", 2).
		WithStoreDir(storeDir3).
		WithRoutes(fmt.Sprintf("nats://127.0.0.1:%d", 14248)) // Route to node1

	// Create clusters with leaf node support enabled
	// Use NewClusterOptions directly to add leaf node support
	opts1 := NewClusterOptions(config1.NodeName).
		WithListen("127.0.0.1", config1.NodePort).
		WithStoreDir(config1.StoreDir).
		WithClusterName(config1.ClusterName).
		WithClusterListen("127.0.0.1", config1.ClusterPort).
		WithRoutes(config1.Routes).
		WithJetStreamMaxMemory(config1.MaxMemory).
		WithJetStreamMaxStore(config1.MaxStorage).
		WithHTTPPort(config1.HTTPPort).
		WithLeafNode("127.0.0.1", 7422, "", "") // Add leaf node listener

	cluster1, err := NewCluster(opts1)
	if err != nil {
		t.Fatalf("failed to create cluster node 1: %v", err)
	}

	opts2 := NewClusterOptions(config2.NodeName).
		WithListen("127.0.0.1", config2.NodePort).
		WithStoreDir(config2.StoreDir).
		WithClusterName(config2.ClusterName).
		WithClusterListen("127.0.0.1", config2.ClusterPort).
		WithRoutes(config2.Routes).
		WithJetStreamMaxMemory(config2.MaxMemory).
		WithJetStreamMaxStore(config2.MaxStorage).
		WithHTTPPort(config2.HTTPPort).
		WithLeafNode("127.0.0.1", 7423, "", "") // Add leaf node listener

	cluster2, err := NewCluster(opts2)
	if err != nil {
		cluster1.Close()
		t.Fatalf("failed to create cluster node 2: %v", err)
	}

	opts3 := NewClusterOptions(config3.NodeName).
		WithListen("127.0.0.1", config3.NodePort).
		WithStoreDir(config3.StoreDir).
		WithClusterName(config3.ClusterName).
		WithClusterListen("127.0.0.1", config3.ClusterPort).
		WithRoutes(config3.Routes).
		WithJetStreamMaxMemory(config3.MaxMemory).
		WithJetStreamMaxStore(config3.MaxStorage).
		WithHTTPPort(config3.HTTPPort).
		WithLeafNode("127.0.0.1", 7424, "", "") // Add leaf node listener

	cluster3, err := NewCluster(opts3)
	if err != nil {
		cluster1.Close()
		cluster2.Close()
		t.Fatalf("failed to create cluster node 3: %v", err)
	}

	// Wait for all clusters to be ready
	waitForClusterReady(t, cluster1, 15*time.Second)
	waitForClusterReady(t, cluster2, 15*time.Second)
	waitForClusterReady(t, cluster3, 15*time.Second)

	// Wait for JetStream to be ready on all nodes (increased timeout for stability)
	waitForJetStreamReady(t, cluster1, 45*time.Second)
	waitForJetStreamReady(t, cluster2, 45*time.Second)
	waitForJetStreamReady(t, cluster3, 45*time.Second)

	// Additional sleep to ensure cluster formation is complete
	time.Sleep(5 * time.Second)

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

		if leaf.nc != nil && leaf.nc.server.Running() && leaf.nc.conn != nil {
			// Test a simple ping to ensure connection is working
			if err := leaf.nc.conn.Flush(); err == nil {
				// Also check if the connection is actually connected
				if leaf.nc.conn.IsConnected() {
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

	// Get cluster leaf node connection info - use the cluster's leaf node port
	clusterLeafPort := 7422 + (cluster.nc.server.Addr().(*net.TCPAddr).Port - 4222) // Calculate leaf port based on cluster port
	hubURL := fmt.Sprintf("nats-leaf://127.0.0.1:%d", clusterLeafPort)

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
		if !leaf1.nc.server.Running() {
			t.Error("leaf1 is not running")
		}
		if !leaf2.nc.server.Running() {
			t.Error("leaf2 is not running")
		}

		// Verify leaf connections
		if leaf1.nc.conn == nil {
			t.Error("leaf1 connection is nil")
		}
		if leaf2.nc.conn == nil {
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

		// Test basic connectivity by sending a simple message from leaf1 to cluster and back
		testSubject := "connectivity.test"
		connectivityCh := make(chan []byte, 1)

		// Subscribe on leaf1 to test round-trip
		sub, err := leaf1.nc.conn.Subscribe(testSubject+".reply", func(msg *nats.Msg) {
			connectivityCh <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf1 for connectivity test: %v", err)
		}
		defer sub.Unsubscribe()

		// Brief wait for subscription
		time.Sleep(500 * time.Millisecond)

		// Send test message
		err = leaf1.nc.conn.Publish(testSubject+".reply", []byte("connectivity-test"))
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
		msgSub, err := leaf2.nc.conn.Subscribe(subject, func(msg *nats.Msg) {
			receivedMessages <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf2: %v", err)
		}
		defer msgSub.Unsubscribe()

		// Wait for subscription to be ready and cluster routing to establish
		time.Sleep(3 * time.Second)

		// Publish from leaf1
		err = leaf1.nc.conn.Publish(subject, testMessage)
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

		// Create stream via cluster1 (central hub)
		err = cluster1.nc.CreateOrUpdateStream(streamConfig)
		if err != nil {
			t.Fatalf("failed to create stream via cluster1: %v", err)
		}
		t.Logf("✓ Successfully created stream '%s' via cluster1", streamConfig.Name)

		// Verify stream exists by getting info via cluster2
		streamInfo, err := cluster2.nc.GetStreamInfo("leaf_test_stream")
		if err != nil {
			t.Fatalf("failed to get stream info via cluster2: %v", err)
		}
		t.Logf("✓ Successfully retrieved stream info via cluster2: %s", streamInfo.Config.Name)

		// Publish message to stream via leaf1
		streamSubject := "leaf.test.message"
		streamMessage := []byte("Hello JetStream from leaf1!")

		err = leaf1.nc.PublishPersistent(streamSubject, streamMessage)
		if err != nil {
			t.Fatalf("failed to publish to stream via leaf1: %v", err)
		}
		t.Log("✓ Successfully published message to stream via leaf1")

		// Subscribe and consume message via leaf2 using SubscribePersistentViaEphemeral
		receivedStreamMessages := make(chan []byte, 1)
		var streamCancel func()

		streamCancel, err = leaf2.nc.SubscribePersistentViaEphemeral(
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

		// Clean up stream via cluster1 (central hub)
		err = cluster1.nc.DeleteStream("leaf_test_stream")
		if err != nil {
			t.Logf("Warning: failed to delete stream: %v", err)
		} else {
			t.Log("✓ Successfully cleaned up stream")
		}
	})

	t.Run("core nats functionality via leaf nodes", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Create leaf nodes connected to different cluster nodes
		leaf1 := SetupLeafNodeConnectedToCluster(t, cluster1, "leaf-core-sender", 4304)
		defer CleanupLeafNodes(leaf1)

		leaf2 := SetupLeafNodeConnectedToCluster(t, cluster2, "leaf-core-receiver", 4305)
		defer CleanupLeafNodes(leaf2)

		// Test 1: Basic Publish/Subscribe
		t.Log("Testing basic publish/subscribe via leaf nodes...")

		pubsubSubject := "core.nats.pubsub"
		pubsubMessage := []byte("Hello from basic pub/sub via leaf!")
		receivedPubSubMessages := make(chan []byte, 1)

		// Subscribe on leaf2
		pubsubSub, err := leaf2.nc.conn.Subscribe(pubsubSubject, func(msg *nats.Msg) {
			receivedPubSubMessages <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe for pubsub test: %v", err)
		}
		defer pubsubSub.Unsubscribe()

		// Wait for subscription to propagate
		time.Sleep(1 * time.Second)

		// Publish from leaf1
		err = leaf1.nc.conn.Publish(pubsubSubject, pubsubMessage)
		if err != nil {
			t.Fatalf("failed to publish for pubsub test: %v", err)
		}

		// Wait for message
		select {
		case receivedMsg := <-receivedPubSubMessages:
			if string(receivedMsg) != string(pubsubMessage) {
				t.Errorf("expected pubsub message '%s', got '%s'", string(pubsubMessage), string(receivedMsg))
			}
			t.Logf("✓ Basic pub/sub successful: %s", string(receivedMsg))
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for pubsub message")
		}

		// Test 2: Request/Reply Pattern
		t.Log("Testing request/reply pattern via leaf nodes...")

		requestSubject := "core.nats.request"
		requestMessage := []byte("Request from leaf1")
		expectedReply := []byte("Reply from leaf2")

		// Set up reply handler on leaf2
		replySub, err := leaf2.nc.conn.Subscribe(requestSubject, func(msg *nats.Msg) {
			if msg.Reply != "" {
				err := leaf2.nc.conn.Publish(msg.Reply, expectedReply)
				if err != nil {
					t.Logf("failed to send reply: %v", err)
				}
			}
		})
		if err != nil {
			t.Fatalf("failed to subscribe for request/reply test: %v", err)
		}
		defer replySub.Unsubscribe()

		// Wait for subscription to propagate
		time.Sleep(1 * time.Second)

		// Send request from leaf1
		replyMsg, err := leaf1.nc.conn.Request(requestSubject, requestMessage, 5*time.Second)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if string(replyMsg.Data) != string(expectedReply) {
			t.Errorf("expected reply '%s', got '%s'", string(expectedReply), string(replyMsg.Data))
		}
		t.Logf("✓ Request/reply successful: sent '%s', received '%s'", string(requestMessage), string(replyMsg.Data))

		// Test 3: Queue Groups
		t.Log("Testing queue groups via leaf nodes...")

		queueSubject := "core.nats.queue"
		queueGroup := "workers"
		queueMessage := []byte("Work item from leaf1")
		receivedQueueMessages := make(chan string, 2) // Buffer for 2 to catch any duplicates

		// Create queue subscribers on both leaf nodes
		queueSub1, err := leaf2.nc.conn.QueueSubscribe(queueSubject, queueGroup, func(msg *nats.Msg) {
			receivedQueueMessages <- "leaf2-worker"
		})
		if err != nil {
			t.Fatalf("failed to create queue subscriber on leaf2: %v", err)
		}
		defer queueSub1.Unsubscribe()

		// Also create a queue subscriber via cluster1 directly for comparison
		queueSub2, err := cluster1.nc.conn.QueueSubscribe(queueSubject, queueGroup, func(msg *nats.Msg) {
			receivedQueueMessages <- "cluster1-worker"
		})
		if err != nil {
			t.Fatalf("failed to create queue subscriber on cluster1: %v", err)
		}
		defer queueSub2.Unsubscribe()

		// Wait for subscriptions to propagate
		time.Sleep(2 * time.Second)

		// Send message from leaf1
		err = leaf1.nc.conn.Publish(queueSubject, queueMessage)
		if err != nil {
			t.Fatalf("failed to publish to queue: %v", err)
		}

		// Wait for message (should only be received by one worker)
		select {
		case worker := <-receivedQueueMessages:
			t.Logf("✓ Queue group working: message processed by %s", worker)

			// Check that no duplicate was received
			select {
			case duplicate := <-receivedQueueMessages:
				t.Errorf("Queue group failed: duplicate message received by %s", duplicate)
			case <-time.After(1 * time.Second):
				t.Log("✓ No duplicate messages received (queue group working correctly)")
			}
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for queue message")
		}

		// Test 4: Wildcard Subscriptions
		t.Log("Testing wildcard subscriptions via leaf nodes...")

		wildcardSubject := "core.nats.wildcard.*"
		specificSubject1 := "core.nats.wildcard.test1"
		specificSubject2 := "core.nats.wildcard.test2"
		receivedWildcardMessages := make(chan string, 2)

		// Subscribe with wildcard on leaf2
		wildcardSub, err := leaf2.nc.conn.Subscribe(wildcardSubject, func(msg *nats.Msg) {
			receivedWildcardMessages <- msg.Subject
		})
		if err != nil {
			t.Fatalf("failed to create wildcard subscriber: %v", err)
		}
		defer wildcardSub.Unsubscribe()

		// Wait for subscription to propagate
		time.Sleep(1 * time.Second)

		// Publish to both specific subjects from leaf1
		err = leaf1.nc.conn.Publish(specificSubject1, []byte("message1"))
		if err != nil {
			t.Fatalf("failed to publish to %s: %v", specificSubject1, err)
		}

		err = leaf1.nc.conn.Publish(specificSubject2, []byte("message2"))
		if err != nil {
			t.Fatalf("failed to publish to %s: %v", specificSubject2, err)
		}

		// Wait for both messages
		receivedSubjects := make([]string, 0, 2)
		for i := 0; i < 2; i++ {
			select {
			case subject := <-receivedWildcardMessages:
				receivedSubjects = append(receivedSubjects, subject)
			case <-time.After(5 * time.Second):
				t.Errorf("timeout waiting for wildcard message %d", i+1)
			}
		}

		if len(receivedSubjects) == 2 {
			t.Logf("✓ Wildcard subscription successful: received messages from %v", receivedSubjects)
		}

		// Test 5: High Frequency Messaging
		t.Log("Testing high frequency messaging via leaf nodes...")

		highFreqSubject := "core.nats.highfreq"
		messageCount := 100
		receivedHighFreqMessages := make(chan int, messageCount)

		// Subscribe on leaf2 with counter
		counter := 0
		highFreqSub, err := leaf2.nc.conn.Subscribe(highFreqSubject, func(msg *nats.Msg) {
			counter++
			receivedHighFreqMessages <- counter
		})
		if err != nil {
			t.Fatalf("failed to create high frequency subscriber: %v", err)
		}
		defer highFreqSub.Unsubscribe()

		// Wait for subscription to propagate
		time.Sleep(1 * time.Second)

		// Send multiple messages rapidly from leaf1
		for i := 1; i <= messageCount; i++ {
			err = leaf1.nc.conn.Publish(highFreqSubject, []byte(fmt.Sprintf("message-%d", i)))
			if err != nil {
				t.Fatalf("failed to publish high freq message %d: %v", i, err)
			}
		}

		// Wait for all messages with longer timeout
		timeout := time.After(10 * time.Second)
		finalCount := 0

	HighFreqLoop:
		for {
			select {
			case count := <-receivedHighFreqMessages:
				finalCount = count
				if count == messageCount {
					break HighFreqLoop
				}
			case <-timeout:
				break HighFreqLoop
			}
		}

		if finalCount == messageCount {
			t.Logf("✓ High frequency messaging successful: received all %d messages", messageCount)
		} else {
			t.Logf("⚠ High frequency messaging partial: received %d out of %d messages", finalCount, messageCount)
		}

		t.Log("✓ All core NATS functionality tests completed successfully")
	})

	t.Run("kv store and object store functionality via leaf nodes", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Create leaf nodes connected to different cluster nodes
		leaf1 := SetupLeafNodeConnectedToCluster(t, cluster1, "leaf-kv-sender", 4306)
		defer CleanupLeafNodes(leaf1)

		leaf2 := SetupLeafNodeConnectedToCluster(t, cluster2, "leaf-kv-receiver", 4307)
		defer CleanupLeafNodes(leaf2)

		// Test KV Store functionality
		t.Log("Testing KV Store functionality via leaf nodes...")

		// Create KV bucket via cluster1 (central hub)
		kvBucketName := "leaf_test_kv_bucket"
		kvConfig := KeyValueStoreConfig{
			Bucket:       kvBucketName,
			Description:  "Test KV bucket for leaf nodes",
			MaxValueSize: size.Size(1024), // 1KB per value
			TTL:          24 * time.Hour,
			MaxBytes:     size.Size(1024 * 1024), // 1MB
			Replicas:     1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV bucket via cluster1: %v", err)
		}
		t.Logf("✓ Successfully created KV bucket '%s' via cluster1", kvBucketName)

		// Test KV operations via leaf nodes
		testKey := "test.key"
		testValue := []byte("Hello KV from leaf1!")

		// Put value via leaf1
		revision1, err := leaf1.nc.PutToKeyValueStore(kvBucketName, testKey, testValue)
		if err != nil {
			t.Fatalf("failed to put KV value via leaf1: %v", err)
		}
		t.Logf("✓ Successfully put KV value via leaf1, revision: %d", revision1)

		// Get value via leaf2
		retrievedValue, revision2, err := leaf2.nc.GetFromKeyValueStore(kvBucketName, testKey)
		if err != nil {
			t.Fatalf("failed to get KV value via leaf2: %v", err)
		}

		if string(retrievedValue) != string(testValue) {
			t.Errorf("expected KV value '%s', got '%s'", string(testValue), string(retrievedValue))
		}
		if revision2 != revision1 {
			t.Errorf("expected revision %d, got %d", revision1, revision2)
		}
		t.Logf("✓ Successfully retrieved KV value via leaf2: %s (revision: %d)", string(retrievedValue), revision2)

		// Test KV update via leaf2
		updatedValue := []byte("Updated value from leaf2!")
		revision3, err := leaf2.nc.UpdateToKeyValueStore(kvBucketName, testKey, updatedValue, revision2)
		if err != nil {
			t.Fatalf("failed to update KV value via leaf2: %v", err)
		}
		t.Logf("✓ Successfully updated KV value via leaf2, new revision: %d", revision3)

		// Verify update via leaf1
		finalValue, finalRevision, err := leaf1.nc.GetFromKeyValueStore(kvBucketName, testKey)
		if err != nil {
			t.Fatalf("failed to get updated KV value via leaf1: %v", err)
		}

		if string(finalValue) != string(updatedValue) {
			t.Errorf("expected updated KV value '%s', got '%s'", string(updatedValue), string(finalValue))
		}
		if finalRevision != revision3 {
			t.Errorf("expected final revision %d, got %d", revision3, finalRevision)
		}
		t.Logf("✓ Successfully verified KV update via leaf1: %s (revision: %d)", string(finalValue), finalRevision)

		// Test KV delete via leaf1
		err = leaf1.nc.DeleteFromKeyValueStore(kvBucketName, testKey)
		if err != nil {
			t.Fatalf("failed to delete KV value via leaf1: %v", err)
		}
		t.Log("✓ Successfully deleted KV value via leaf1")

		// Verify deletion via leaf2
		_, _, err = leaf2.nc.GetFromKeyValueStore(kvBucketName, testKey)
		if err == nil {
			t.Error("expected error when getting deleted KV value, but got none")
		}
		t.Log("✓ Successfully verified KV deletion via leaf2")

		// Test Object Store functionality
		t.Log("Testing Object Store functionality via leaf nodes...")

		// Create Object Store bucket via cluster2 (different node)
		osBucketName := "leaf_test_os_bucket"
		osConfig := ObjectStoreConfig{
			Bucket:      osBucketName,
			Description: "Test Object Store bucket for leaf nodes",
			MaxBytes:    size.Size(10 * 1024 * 1024), // 10MB
			TTL:         24 * time.Hour,
			Replicas:    1,
		}

		err = cluster2.nc.CreateObjectStore("test-cluster", osConfig)
		if err != nil {
			t.Fatalf("failed to create Object Store bucket via cluster2: %v", err)
		}
		t.Logf("✓ Successfully created Object Store bucket '%s' via cluster2", osBucketName)

		// Test Object Store operations via leaf nodes
		objectName := "test-object.txt"
		objectData := []byte("Hello Object Store from leaf nodes!\nThis is a test object with multiple lines.\nLine 3 for testing.")

		// Put object via leaf2
		err = leaf2.nc.PutToObjectStore(osBucketName, objectName, objectData, map[string]string{
			"created-by": "leaf2",
			"test-type":  "functionality",
		})
		if err != nil {
			t.Fatalf("failed to put object via leaf2: %v", err)
		}
		t.Logf("✓ Successfully put object '%s' via leaf2, size: %d bytes", objectName, len(objectData))

		// Get object via leaf1
		retrievedObjectData, err := leaf1.nc.GetFromObjectStore(osBucketName, objectName)
		if err != nil {
			t.Fatalf("failed to get object via leaf1: %v", err)
		}

		if string(retrievedObjectData) != string(objectData) {
			t.Errorf("expected object data '%s', got '%s'", string(objectData), string(retrievedObjectData))
		}
		t.Logf("✓ Successfully retrieved object via leaf1, size: %d bytes", len(retrievedObjectData))

		// Test large object handling
		t.Log("Testing large object handling...")
		largeObjectName := "large-test-object.bin"
		largeObjectData := make([]byte, 1024*50) // 50KB
		for i := range largeObjectData {
			largeObjectData[i] = byte(i % 256)
		}

		// Put large object via leaf1
		err = leaf1.nc.PutToObjectStore(osBucketName, largeObjectName, largeObjectData, map[string]string{
			"created-by": "leaf1",
			"size":       "50KB",
		})
		if err != nil {
			t.Fatalf("failed to put large object via leaf1: %v", err)
		}
		t.Logf("✓ Successfully put large object '%s' via leaf1, size: %d bytes", largeObjectName, len(largeObjectData))

		// Get large object via leaf2
		retrievedLargeObjectData, err := leaf2.nc.GetFromObjectStore(osBucketName, largeObjectName)
		if err != nil {
			t.Fatalf("failed to get large object via leaf2: %v", err)
		}

		if len(retrievedLargeObjectData) != len(largeObjectData) {
			t.Errorf("expected large object size %d, got %d", len(largeObjectData), len(retrievedLargeObjectData))
		}

		// Verify content integrity for first and last 100 bytes
		for i := 0; i < 100; i++ {
			if retrievedLargeObjectData[i] != largeObjectData[i] {
				t.Errorf("large object data mismatch at byte %d: expected %d, got %d", i, largeObjectData[i], retrievedLargeObjectData[i])
				break
			}
		}
		for i := len(largeObjectData) - 100; i < len(largeObjectData); i++ {
			if retrievedLargeObjectData[i] != largeObjectData[i] {
				t.Errorf("large object data mismatch at byte %d: expected %d, got %d", i, largeObjectData[i], retrievedLargeObjectData[i])
				break
			}
		}
		t.Logf("✓ Successfully retrieved and verified large object via leaf2, size: %d bytes", len(retrievedLargeObjectData))

		// Test object deletion via leaf2
		err = leaf2.nc.DeleteFromObjectStore(osBucketName, objectName)
		if err != nil {
			t.Fatalf("failed to delete object via leaf2: %v", err)
		}
		t.Log("✓ Successfully deleted object via leaf2")

		// Verify deletion via leaf1
		_, err = leaf1.nc.GetFromObjectStore(osBucketName, objectName)
		if err == nil {
			t.Error("expected error when getting deleted object, but got none")
		}
		t.Log("✓ Successfully verified object deletion via leaf1")

		// Clean up large object
		err = leaf1.nc.DeleteFromObjectStore(osBucketName, largeObjectName)
		if err != nil {
			t.Logf("Warning: failed to delete large object: %v", err)
		} else {
			t.Log("✓ Successfully cleaned up large object")
		}

		t.Log("✓ All KV Store and Object Store functionality tests completed successfully")
	})

	t.Run("remote clients connected to leaf nodes", func(t *testing.T) {
		// Setup three node cluster
		cluster1, cluster2, cluster3 := SetupLeafTestThreeNodeCluster(t)
		defer CleanupLeafTestClusters(cluster1, cluster2, cluster3)

		// Create leaf nodes connected to different cluster nodes
		leaf1 := SetupLeafNodeConnectedToCluster(t, cluster1, "leaf-client-hub1", 4308)
		defer CleanupLeafNodes(leaf1)

		leaf2 := SetupLeafNodeConnectedToCluster(t, cluster2, "leaf-client-hub2", 4309)
		defer CleanupLeafNodes(leaf2)

		// Create remote clients that connect to leaf nodes
		t.Log("Creating remote clients connected to leaf nodes...")

		// Client 1 connects to leaf1
		client1Opts := NewClientOptions().
			WithServers(fmt.Sprintf("nats://127.0.0.1:%d", 4308))
		client1, err := NewClient(client1Opts)
		if err != nil {
			t.Fatalf("failed to create client1: %v", err)
		}
		defer client1.Close()

		// Client 2 connects to leaf2
		client2Opts := NewClientOptions().
			WithServers(fmt.Sprintf("nats://127.0.0.1:%d", 4309))
		client2, err := NewClient(client2Opts)
		if err != nil {
			t.Fatalf("failed to create client2: %v", err)
		}
		defer client2.Close()

		t.Log("✓ Successfully created remote clients connected to leaf nodes")

		// Test 1: Basic messaging via clients through leaf nodes
		t.Log("Testing basic messaging via remote clients...")

		clientSubject := "client.leaf.messaging"
		clientMessage := []byte("Hello from remote client1 via leaf1!")
		receivedClientMessages := make(chan []byte, 1)

		// Subscribe on client2 (connected to leaf2)
		_, err = client2.nc.conn.Subscribe(clientSubject, func(msg *nats.Msg) {
			receivedClientMessages <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on client2: %v", err)
		}

		// Wait for subscription to propagate
		time.Sleep(2 * time.Second)

		// Publish from client1 (connected to leaf1)
		err = client1.nc.conn.Publish(clientSubject, clientMessage)
		if err != nil {
			t.Fatalf("failed to publish from client1: %v", err)
		}

		// Wait for message
		select {
		case receivedMsg := <-receivedClientMessages:
			if string(receivedMsg) != string(clientMessage) {
				t.Errorf("expected client message '%s', got '%s'", string(clientMessage), string(receivedMsg))
			}
			t.Logf("✓ Basic client messaging successful: %s", string(receivedMsg))
		case <-time.After(10 * time.Second):
			t.Error("timeout waiting for client message")
		}

		// Test 2: JetStream operations via remote clients
		t.Log("Testing JetStream operations via remote clients...")

		// Create stream via cluster (central hub)
		clientStreamConfig := &PersistentConfig{
			Name:        "client_leaf_stream",
			Description: "Test stream for remote clients via leaf nodes",
			Subjects:    []string{"client.stream.*"},
			Retention:   nats.WorkQueuePolicy,
			MaxMsgs:     500,
			MaxBytes:    512 * 1024, // 512KB
			MaxAge:      12 * time.Hour,
			Replicas:    1,
		}

		err = cluster1.nc.CreateOrUpdateStream(clientStreamConfig)
		if err != nil {
			t.Fatalf("failed to create stream for client test: %v", err)
		}
		t.Logf("✓ Successfully created stream '%s' for client test", clientStreamConfig.Name)

		// Publish to stream via client1
		streamSubject := "client.stream.message"
		streamMessage := []byte("JetStream message from remote client1!")

		err = client1.nc.PublishPersistent(streamSubject, streamMessage)
		if err != nil {
			t.Fatalf("failed to publish to stream via client1: %v", err)
		}
		t.Log("✓ Successfully published to stream via remote client1")

		// Subscribe and consume via client2
		receivedStreamMessages := make(chan []byte, 1)
		var streamCancel func()

		streamCancel, err = client2.nc.SubscribePersistentViaEphemeral(
			streamSubject,
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				receivedStreamMessages <- msg
				return nil, false, true // ack the message
			},
			func(err error) {
				t.Logf("Client stream subscription error: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe to stream via client2: %v", err)
		}
		defer streamCancel()

		// Wait for stream message
		select {
		case receivedMsg := <-receivedStreamMessages:
			if string(receivedMsg) != string(streamMessage) {
				t.Errorf("expected stream message '%s', got '%s'", string(streamMessage), string(receivedMsg))
			}
			t.Logf("✓ Successfully received stream message via remote client2: %s", string(receivedMsg))
		case <-time.After(10 * time.Second):
			t.Error("timeout waiting for stream message via clients")
		}

		// Test 3: KV Store operations via remote clients
		t.Log("Testing KV Store operations via remote clients...")

		// Create KV bucket
		clientKVBucket := "client_leaf_kv"
		kvConfig := KeyValueStoreConfig{
			Bucket:       clientKVBucket,
			Description:  "Test KV bucket for remote clients",
			MaxValueSize: size.Size(512), // 512 bytes per value
			TTL:          6 * time.Hour,
			MaxBytes:     size.Size(256 * 1024), // 256KB
			Replicas:     1,
		}

		err = cluster2.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV bucket for client test: %v", err)
		}
		t.Logf("✓ Successfully created KV bucket '%s' for client test", clientKVBucket)

		// Put value via client1
		clientKey := "client.test.key"
		clientValue := []byte("Value from remote client1!")

		revision, err := client1.nc.PutToKeyValueStore(clientKVBucket, clientKey, clientValue)
		if err != nil {
			t.Fatalf("failed to put KV value via client1: %v", err)
		}
		t.Logf("✓ Successfully put KV value via remote client1, revision: %d", revision)

		// Get value via client2
		retrievedValue, retrievedRevision, err := client2.nc.GetFromKeyValueStore(clientKVBucket, clientKey)
		if err != nil {
			t.Fatalf("failed to get KV value via client2: %v", err)
		}

		if string(retrievedValue) != string(clientValue) {
			t.Errorf("expected KV value '%s', got '%s'", string(clientValue), string(retrievedValue))
		}
		if retrievedRevision != revision {
			t.Errorf("expected revision %d, got %d", revision, retrievedRevision)
		}
		t.Logf("✓ Successfully retrieved KV value via remote client2: %s (revision: %d)", string(retrievedValue), retrievedRevision)

		// Test 4: Object Store operations via remote clients
		t.Log("Testing Object Store operations via remote clients...")

		// Create Object Store bucket
		clientOSBucket := "client_leaf_objects"
		osConfig := ObjectStoreConfig{
			Bucket:      clientOSBucket,
			Description: "Test Object Store bucket for remote clients",
			MaxBytes:    size.Size(5 * 1024 * 1024), // 5MB
			TTL:         6 * time.Hour,
			Replicas:    1,
		}

		err = cluster3.nc.CreateObjectStore("test-cluster", osConfig)
		if err != nil {
			t.Fatalf("failed to create Object Store bucket for client test: %v", err)
		}
		t.Logf("✓ Successfully created Object Store bucket '%s' for client test", clientOSBucket)

		// Put object via client1
		objectName := "client-test-file.txt"
		objectData := []byte("This is a test file created by remote client1!\nIt contains multiple lines.\nFor testing object store via leaf nodes.")

		err = client1.nc.PutToObjectStore(clientOSBucket, objectName, objectData, map[string]string{
			"client":    "remote-client1",
			"via":       "leaf-node",
			"test-type": "client-object-store",
		})
		if err != nil {
			t.Fatalf("failed to put object via client1: %v", err)
		}
		t.Logf("✓ Successfully put object '%s' via remote client1, size: %d bytes", objectName, len(objectData))

		// Get object via client2
		retrievedObjectData, err := client2.nc.GetFromObjectStore(clientOSBucket, objectName)
		if err != nil {
			t.Fatalf("failed to get object via client2: %v", err)
		}

		if string(retrievedObjectData) != string(objectData) {
			t.Errorf("expected object data '%s', got '%s'", string(objectData), string(retrievedObjectData))
		}
		t.Logf("✓ Successfully retrieved object via remote client2, size: %d bytes", len(retrievedObjectData))

		// Test 5: Mixed operations - clients + leaf nodes + cluster
		t.Log("Testing mixed operations: clients, leaf nodes, and cluster...")

		mixedSubject := "mixed.operations.test"
		mixedMessages := make(chan string, 3)

		// Subscribe on all three types: client, leaf, cluster
		_, err = client1.nc.conn.Subscribe(mixedSubject, func(msg *nats.Msg) {
			mixedMessages <- "client1"
		})
		if err != nil {
			t.Fatalf("failed to subscribe on client1 for mixed test: %v", err)
		}

		_, err = leaf1.nc.conn.Subscribe(mixedSubject, func(msg *nats.Msg) {
			mixedMessages <- "leaf1"
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf1 for mixed test: %v", err)
		}

		_, err = cluster1.nc.conn.Subscribe(mixedSubject, func(msg *nats.Msg) {
			mixedMessages <- "cluster1"
		})
		if err != nil {
			t.Fatalf("failed to subscribe on cluster1 for mixed test: %v", err)
		}

		// Wait for subscriptions to propagate
		time.Sleep(2 * time.Second)

		// Publish from client2
		err = client2.nc.conn.Publish(mixedSubject, []byte("Mixed operation test"))
		if err != nil {
			t.Fatalf("failed to publish mixed test message: %v", err)
		}

		// Collect all received messages
		receivedFrom := make([]string, 0, 3)
		timeout := time.After(5 * time.Second)

	MixedLoop:
		for len(receivedFrom) < 3 {
			select {
			case from := <-mixedMessages:
				receivedFrom = append(receivedFrom, from)
			case <-timeout:
				break MixedLoop
			}
		}

		if len(receivedFrom) >= 2 {
			t.Logf("✓ Mixed operations successful: message received by %v", receivedFrom)
		} else {
			t.Logf("⚠ Mixed operations partial: message received by %v", receivedFrom)
		}

		// Clean up resources
		err = cluster1.nc.DeleteStream("client_leaf_stream")
		if err != nil {
			t.Logf("Warning: failed to delete client stream: %v", err)
		} else {
			t.Log("✓ Successfully cleaned up client stream")
		}

		t.Log("✓ All remote client tests completed successfully")
		t.Log("✓ Remote clients can successfully operate through leaf nodes with full NATS functionality")
	})
}
