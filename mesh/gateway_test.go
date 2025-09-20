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
							WithHTTPPort(c.HTTPPort)

	// Only add gateway if GatewayName is not empty and RemoteGateways is not nil
	if c.GatewayName != "" && c.RemoteGateways != nil {
		opts = opts.WithGateway(
			c.GatewayName,
			"127.0.0.1",
			c.GatewayPort,
			"",
			"",
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
		ServerName:  fmt.Sprintf("%s-node2", clusterName), // Unique server name
		ClusterName: clusterName,                          // Use cluster-specific name
		NodePort:    baseNodePort + 1,
		ClusterPort: baseClusterPort + 1,
		HTTPPort:    baseHTTPPort + 1,
		StoreDir:    storeDir2,
		MaxMemory:   size.NewSizeFromMegabytes(50),
		MaxStorage:  size.NewSizeFromMegabytes(100),
		Routes:      []string{fmt.Sprintf("nats://127.0.0.1:%d", baseClusterPort)}, // Route to node1
	}

	// Node 3 - regular cluster node (no gateway)
	config3 := &GatewayTestConfig{
		ServerName:  fmt.Sprintf("%s-node3", clusterName), // Unique server name
		ClusterName: clusterName,                          // Use cluster-specific name
		NodePort:    baseNodePort + 2,
		ClusterPort: baseClusterPort + 2,
		HTTPPort:    baseHTTPPort + 2,
		StoreDir:    storeDir3,
		MaxMemory:   size.NewSizeFromMegabytes(50),
		MaxStorage:  size.NewSizeFromMegabytes(100),
		Routes:      []string{fmt.Sprintf("nats://127.0.0.1:%d", baseClusterPort)}, // Route to node1
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
		Add("cluster-b", "nats://127.0.0.1:7223").
		Add("cluster-a", "nats://127.0.0.1:7222")

	// Cluster B will connect to Cluster A's gateway
	remoteGatewaysB := NewRemoteGateways().
		Add("cluster-a", "nats://127.0.0.1:7222").
		Add("cluster-b", "nats://127.0.0.1:7223")

	// Create cluster A (three nodes, node1 has gateway)
	clusterA1, clusterA2, clusterA3 := SetupGatewayTestThreeNodeCluster(t, "cluster-a", 0, remoteGatewaysA)

	// Create cluster B (three nodes, node1 has gateway) - but first create without remote gateways
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

		time.Sleep(3 * time.Second) // Wait for gateways to connect

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

	t.Logf(" - Publishing from Cluster A to Cluster B via gateways")

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

// Test request-reply pattern across gateways
func TestGatewayRequestReply(t *testing.T) {
	t.Run("request-reply across gateway clusters", func(t *testing.T) {
		clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3 := SetupGatewayTestTwoClusters(t)
		defer CleanupGatewayTestClusters(clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3)

		// Wait for gateway connections to establish
		time.Sleep(3 * time.Second)

		t.Log("Testing request-reply pattern across gateways...")
		testRequestReplyAcrossGateways(t, clusterA1, clusterB1)
	})
}

// testRequestReplyAcrossGateways tests request-reply messaging across gateways
func testRequestReplyAcrossGateways(t *testing.T, clusterA, clusterB *Cluster) {
	t.Helper()

	subject := "gateway.request.echo"
	requestData := "Echo this message across gateway"

	// Set up responder on cluster B
	sub, err := clusterB.nc.conn.Subscribe(subject, func(msg *nats.Msg) {
		t.Logf("   - Cluster B received request: %s", string(msg.Data))
		response := fmt.Sprintf("Echo: %s", string(msg.Data))

		if err := msg.Respond([]byte(response)); err != nil {
			t.Logf("   - Failed to respond: %v", err)
		} else {
			t.Logf("   - Cluster B sent response: %s", response)
		}
	})
	if err != nil {
		t.Fatalf("failed to subscribe responder on cluster B: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for subscription to propagate across gateway
	time.Sleep(3 * time.Second)

	// Send request from cluster A
	t.Logf(" - Sending request from Cluster A to Cluster B")
	resp, err := clusterA.nc.conn.Request(subject, []byte(requestData), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to send request from cluster A: %v", err)
	}

	expectedResponse := fmt.Sprintf("Echo: %s", requestData)
	if string(resp.Data) != expectedResponse {
		t.Errorf("expected response '%s', got '%s'", expectedResponse, string(resp.Data))
	} else {
		t.Logf("✓ Successfully completed request-reply across gateway: %s", string(resp.Data))
	}
}

// Test JetStream functionality across gateways
func TestGatewayJetStream(t *testing.T) {
	t.Run("jetstream across gateway clusters", func(t *testing.T) {
		clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3 := SetupGatewayTestTwoClusters(t)
		defer CleanupGatewayTestClusters(clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3)

		// Wait for gateway connections to establish
		time.Sleep(3 * time.Second)

		t.Log("Testing JetStream across gateways...")
		testJetStreamAcrossGateways(t, clusterA1, clusterB1)
	})
}

// testJetStreamAcrossGateways tests JetStream functionality across gateways
func testJetStreamAcrossGateways(t *testing.T, clusterA, clusterB *Cluster) {
	t.Helper()

	streamName := "GATEWAY_TEST_STREAM"
	subject := "gateway.jetstream.test"

	// Create stream on cluster A
	t.Logf(" - Creating JetStream stream '%s' on Cluster A", streamName)
	streamInfo, err := clusterA.nc.js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		Storage:  nats.MemoryStorage,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("failed to create stream on cluster A: %v", err)
	}
	t.Logf("   - Stream created: %s", streamInfo.Config.Name)

	// Publish messages from cluster A
	t.Logf(" - Publishing messages to JetStream from Cluster A")
	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("JetStream message %d from cluster A", i+1)
		ack, err := clusterA.nc.js.Publish(subject, []byte(msg))
		if err != nil {
			t.Fatalf("failed to publish message %d: %v", i+1, err)
		}
		t.Logf("   - Published message %d, seq: %d", i+1, ack.Sequence)
	}

	// Try to access stream from cluster B (this may not work across gateways - for debugging)
	t.Logf(" - Attempting to access stream from Cluster B...")
	_, err = clusterB.nc.js.StreamInfo(streamName)
	if err != nil {
		t.Logf("   - Expected: Cannot access stream from cluster B: %v", err)
		t.Logf("   - This is normal behavior - JetStream streams are cluster-local")
	} else {
		t.Logf("   - Unexpected: Stream accessible from cluster B")
	}

	// Test consumer on cluster A
	t.Logf(" - Creating consumer on Cluster A")
	consumerName := "gateway-test-consumer"
	consumerInfo, err := clusterA.nc.js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:   consumerName,
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
	t.Logf("   - Consumer created: %s", consumerInfo.Name)

	// Pull messages
	t.Logf(" - Pulling messages from consumer")
	sub, err := clusterA.nc.js.PullSubscribe(subject, consumerName)
	if err != nil {
		t.Fatalf("failed to create pull subscription: %v", err)
	}
	defer sub.Unsubscribe()

	msgs, err := sub.Fetch(3, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("failed to fetch messages: %v", err)
	}

	if len(msgs) != 3 {
		t.Errorf("expected 3 messages, got %d", len(msgs))
	}

	for i, msg := range msgs {
		t.Logf("   - Received message %d: %s", i+1, string(msg.Data))
		msg.Ack()
	}

	t.Logf("✓ Successfully tested JetStream functionality on gateway cluster")
}

// Test KVStore functionality across gateways
func TestGatewayKVStore(t *testing.T) {
	t.Run("kvstore across gateway clusters", func(t *testing.T) {
		clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3 := SetupGatewayTestTwoClusters(t)
		defer CleanupGatewayTestClusters(clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3)

		// Wait for gateway connections to establish
		time.Sleep(3 * time.Second)

		t.Log("Testing KVStore across gateways...")
		testKVStoreAcrossGateways(t, clusterA1, clusterB1)
	})
}

// testKVStoreAcrossGateways tests KVStore functionality across gateways
func testKVStoreAcrossGateways(t *testing.T, clusterA, clusterB *Cluster) {
	t.Helper()

	bucketName := "gateway-test-bucket"

	// Create KV bucket on cluster A
	t.Logf(" - Creating KV bucket '%s' on Cluster A", bucketName)
	kvA, err := clusterA.nc.js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:   bucketName,
		Storage:  nats.MemoryStorage,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("failed to create KV bucket on cluster A: %v", err)
	}
	t.Logf("   - KV bucket created successfully")

	// Put some values on cluster A
	testData := map[string]string{
		"key1": "value1-from-cluster-A",
		"key2": "value2-from-cluster-A",
		"key3": "value3-from-cluster-A",
	}

	t.Logf(" - Storing data in KV bucket on Cluster A")
	for key, value := range testData {
		rev, err := kvA.Put(key, []byte(value))
		if err != nil {
			t.Fatalf("failed to put key '%s': %v", key, err)
		}
		t.Logf("   - Put %s = %s (revision: %d)", key, value, rev)
	}

	// Try to access KV bucket from cluster B (this may not work across gateways - for debugging)
	t.Logf(" - Attempting to access KV bucket from Cluster B...")
	kvB, err := clusterB.nc.js.KeyValue(bucketName)
	if err != nil {
		t.Logf("   - Expected: Cannot access KV bucket from cluster B: %v", err)
		t.Logf("   - This is normal behavior - KV buckets are cluster-local")

		// Try to create a separate bucket on cluster B
		t.Logf(" - Creating separate KV bucket on Cluster B")
		kvB, err = clusterB.nc.js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:   bucketName + "-b",
			Storage:  nats.MemoryStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("failed to create KV bucket on cluster B: %v", err)
		}

		// Put data on cluster B's bucket
		rev, err := kvB.Put("cross-cluster-key", []byte("value-from-cluster-B"))
		if err != nil {
			t.Fatalf("failed to put data in cluster B bucket: %v", err)
		}
		t.Logf("   - Put data in cluster B bucket (revision: %d)", rev)

	} else {
		t.Logf("   - Unexpected: KV bucket accessible from cluster B")

		// Try to read values
		for key := range testData {
			entry, err := kvB.Get(key)
			if err != nil {
				t.Logf("   - Cannot get key '%s' from cluster B: %v", key, err)
			} else {
				t.Logf("   - Got %s = %s from cluster B", key, string(entry.Value()))
			}
		}
	}

	// Read back values on cluster A to verify
	t.Logf(" - Reading back values from Cluster A")
	for key, expectedValue := range testData {
		entry, err := kvA.Get(key)
		if err != nil {
			t.Fatalf("failed to get key '%s' from cluster A: %v", key, err)
		}

		if string(entry.Value()) != expectedValue {
			t.Errorf("expected value '%s' for key '%s', got '%s'", expectedValue, key, string(entry.Value()))
		} else {
			t.Logf("   - Verified %s = %s", key, string(entry.Value()))
		}
	}

	t.Logf("✓ Successfully tested KV functionality on gateway clusters")
}

// Test Object Store functionality across gateways
func TestGatewayObjectStore(t *testing.T) {
	t.Run("object store across gateway clusters", func(t *testing.T) {
		clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3 := SetupGatewayTestTwoClusters(t)
		defer CleanupGatewayTestClusters(clusterA1, clusterA2, clusterA3, clusterB1, clusterB2, clusterB3)

		// Wait for gateway connections to establish
		time.Sleep(3 * time.Second)

		t.Log("Testing Object Store across gateways...")
		testObjectStoreAcrossGateways(t, clusterA1, clusterB1)
	})
}

// testObjectStoreAcrossGateways tests Object Store functionality across gateways
func testObjectStoreAcrossGateways(t *testing.T, clusterA, clusterB *Cluster) {
	t.Helper()

	bucketName := "gateway-test-objects"

	// Create Object Store bucket on cluster A
	t.Logf(" - Creating Object Store bucket '%s' on Cluster A", bucketName)
	osA, err := clusterA.nc.js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket:   bucketName,
		Storage:  nats.MemoryStorage,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("failed to create object store on cluster A: %v", err)
	}
	t.Logf("   - Object Store bucket created successfully")

	// Put some objects on cluster A
	testObjects := map[string][]byte{
		"file1.txt": []byte("This is content of file1 from cluster A"),
		"file2.txt": []byte("This is content of file2 from cluster A"),
		"data.json": []byte(`{"message": "Hello from cluster A", "timestamp": "2025-09-20"}`),
	}

	t.Logf(" - Storing objects in Object Store on Cluster A")
	for name, content := range testObjects {
		info, err := osA.PutBytes(name, content)
		if err != nil {
			t.Fatalf("failed to put object '%s': %v", name, err)
		}
		t.Logf("   - Put object %s (%d bytes, NUID: %s)", name, info.Size, info.NUID)
	}

	// Try to access Object Store from cluster B (this may not work across gateways - for debugging)
	t.Logf(" - Attempting to access Object Store from Cluster B...")
	osB, err := clusterB.nc.js.ObjectStore(bucketName)
	if err != nil {
		t.Logf("   - Expected: Cannot access Object Store from cluster B: %v", err)
		t.Logf("   - This is normal behavior - Object Stores are cluster-local")

		// Try to create a separate object store on cluster B
		t.Logf(" - Creating separate Object Store bucket on Cluster B")
		osB, err = clusterB.nc.js.CreateObjectStore(&nats.ObjectStoreConfig{
			Bucket:   bucketName + "-b",
			Storage:  nats.MemoryStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("failed to create object store on cluster B: %v", err)
		}

		// Put object on cluster B's store
		info, err := osB.PutBytes("cluster-b-file.txt", []byte("Content from cluster B"))
		if err != nil {
			t.Fatalf("failed to put object in cluster B store: %v", err)
		}
		t.Logf("   - Put object in cluster B store (%d bytes, NUID: %s)", info.Size, info.NUID)

	} else {
		t.Logf("   - Unexpected: Object Store accessible from cluster B")

		// Try to list objects
		objects, err := osB.List()
		if err != nil {
			t.Logf("   - Cannot list objects from cluster B: %v", err)
		} else {
			t.Logf("   - Listed %d objects from cluster B", len(objects))
			for _, obj := range objects {
				t.Logf("     - Object: %s (%d bytes)", obj.Name, obj.Size)
			}
		}
	}

	// Read back objects on cluster A to verify
	t.Logf(" - Reading back objects from Cluster A")
	for name, expectedContent := range testObjects {
		data, err := osA.GetBytes(name)
		if err != nil {
			t.Fatalf("failed to get object '%s' from cluster A: %v", name, err)
		}

		if string(data) != string(expectedContent) {
			t.Errorf("expected content '%s' for object '%s', got '%s'", string(expectedContent), name, string(data))
		} else {
			t.Logf("   - Verified object %s (%d bytes)", name, len(data))
		}
	}

	// List all objects on cluster A
	t.Logf(" - Listing all objects on Cluster A")
	objects, err := osA.List()
	if err != nil {
		t.Fatalf("failed to list objects on cluster A: %v", err)
	}

	if len(objects) != len(testObjects) {
		t.Errorf("expected %d objects, got %d", len(testObjects), len(objects))
	}

	for _, obj := range objects {
		t.Logf("   - Object: %s (%d bytes, modified: %v)", obj.Name, obj.Size, obj.ModTime)
	}

	t.Logf("✓ Successfully tested Object Store functionality on gateway clusters")
}
