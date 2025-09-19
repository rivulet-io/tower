package mesh

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rivulet-io/tower/util/size"
)

// LeafTestConfig holds configuration for test leaf setup
type LeafTestConfig struct {
	NodeName   string
	Host       string
	Port       int
	Username   string
	Password   string
	RemoteURLs [][]string
}

// DefaultLeafTestConfig creates a default test configuration for leaf node
func DefaultLeafTestConfig(nodeName string, nodeIndex int) *LeafTestConfig {
	basePort := 4300

	return &LeafTestConfig{
		NodeName: nodeName,
		Host:     "127.0.0.1",
		Port:     basePort + nodeIndex,
	}
}

// WithAuth sets the authentication for the leaf configuration
func (c *LeafTestConfig) WithAuth(username, password string) *LeafTestConfig {
	c.Username = username
	c.Password = password
	return c
}

// WithRemotes sets the remote URLs for the leaf configuration
func (c *LeafTestConfig) WithRemotes(remotes ...[]string) *LeafTestConfig {
	c.RemoteURLs = remotes
	return c
}

// CreateLeaf creates a leaf from the test configuration
func (c *LeafTestConfig) CreateLeaf() (*Leaf, error) {
	opts := NewLeafOptions(c.NodeName).
		WithListen(c.Host, c.Port)

	if c.Username != "" && c.Password != "" {
		opts = opts.WithLeafAuth(c.Username, c.Password)
	}

	if len(c.RemoteURLs) > 0 {
		opts = opts.WithLeafRemotes(c.RemoteURLs...)
	}

	return NewLeaf(opts)
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
				return
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

// SetupSimpleHub creates a simple NATS hub without JetStream for leaf testing
func SetupSimpleHub(t *testing.T) *Cluster {
	t.Helper()

	// Create minimal cluster options without JetStream
	opts := NewClusterOptions("simple-hub").
		WithListen("127.0.0.1", 4222).
		WithStoreDir(t.TempDir())
	// Don't add JetStream configurations to keep it simple

	cluster, err := NewCluster(opts)
	if err != nil {
		t.Fatalf("failed to create simple hub: %v", err)
	}

	// Wait for basic server to be ready (no JetStream)
	start := time.Now()
	for {
		if time.Since(start) > 10*time.Second {
			t.Fatalf("simple hub not ready within timeout")
		}

		if cluster.nc.server.Running() && cluster.nc.conn != nil {
			if err := cluster.nc.conn.Flush(); err == nil {
				break
			}
		}

		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)
	return cluster
}

// SetupSingleNodeCluster creates a hub cluster for leaf nodes to connect to
func SetupSingleNodeCluster(t *testing.T) *Cluster {
	t.Helper()

	return SetupSimpleHub(t)
}

// SetupLeafConnectedToHub creates a leaf node connected to a hub cluster
func SetupLeafConnectedToHub(t *testing.T, hub *Cluster) *Leaf {
	t.Helper()

	// Use known hub port (4222 is the default from DefaultClusterTestConfig)
	hubURL := "nats://127.0.0.1:4222"

	config := DefaultLeafTestConfig("test-leaf", 0).
		WithRemotes([]string{hubURL})

	leaf, err := config.CreateLeaf()
	if err != nil {
		t.Fatalf("failed to create leaf node: %v", err)
	}

	waitForLeafReady(t, leaf, 5*time.Second)

	// Shorter wait time for leaf-hub connection establishment
	time.Sleep(500 * time.Millisecond)

	return leaf
}

// CleanupLeaf safely closes a leaf node
func CleanupLeaf(leaf *Leaf) {
	if leaf != nil {
		leaf.Close()
	}
}

func TestLeafNodeCreation(t *testing.T) {
	t.Run("standalone leaf creation", func(t *testing.T) {
		// Test standalone leaf without hub connection
		config := DefaultLeafTestConfig("standalone-leaf", 0)

		leaf, err := config.CreateLeaf()
		if err != nil {
			t.Fatalf("failed to create standalone leaf: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 5*time.Second)

		// Verify leaf is running
		if !leaf.conn.server.Running() {
			t.Error("standalone leaf server is not running")
		}

		if leaf.conn.conn == nil {
			t.Error("standalone leaf connection is nil")
		}

		t.Log("Successfully created standalone leaf node")
	})

	t.Run("leaf with authentication", func(t *testing.T) {
		config := DefaultLeafTestConfig("auth-leaf", 1).
			WithAuth("testuser", "testpass")

		leaf, err := config.CreateLeaf()
		if err != nil {
			t.Fatalf("failed to create leaf with auth: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 5*time.Second)

		// Verify leaf is running
		if !leaf.conn.server.Running() {
			t.Error("leaf server with auth is not running")
		}

		t.Log("Successfully created leaf node with authentication")
	})

	t.Run("basic leaf creation with hub", func(t *testing.T) {
		// Setup hub cluster first
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Create leaf connected to hub
		leaf := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf)

		// Verify leaf is running
		if !leaf.conn.server.Running() {
			t.Error("leaf server is not running")
		}

		if leaf.conn.conn == nil {
			t.Error("leaf connection is nil")
		}

		// Verify hub is running
		if !hub.nc.server.Running() {
			t.Error("hub server is not running")
		}

		t.Log("Successfully created basic leaf node connected to hub")
	})
}

func TestLeafNodeConnection(t *testing.T) {
	t.Run("leaf connects to hub", func(t *testing.T) {
		// Setup hub cluster
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Setup leaf node connected to hub
		leaf := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf)

		// Verify both are running
		if !hub.nc.server.Running() {
			t.Error("hub cluster is not running")
		}
		if !leaf.conn.server.Running() {
			t.Error("leaf node is not running")
		}

		// Verify leaf has connection to hub
		if leaf.conn.conn == nil {
			t.Error("leaf connection is nil")
		}

		t.Log("Successfully connected leaf node to hub")
	})

	t.Run("leaf to leaf communication via hub", func(t *testing.T) {
		// Setup hub cluster
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Setup two leaf nodes
		leaf1 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf1)

		leaf2 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf2)

		// Test message routing between leaves through hub
		subject := "test.leaf.communication"
		testMessage := []byte("Hello from leaf1 to leaf2")

		// Subscribe on leaf2
		receivedMessages := make(chan []byte, 1)
		sub, err := leaf2.conn.conn.Subscribe(subject, func(msg *nats.Msg) {
			receivedMessages <- msg.Data
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf2: %v", err)
		}
		defer sub.Unsubscribe()

		// Wait for subscription to be ready
		time.Sleep(500 * time.Millisecond)

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
			t.Logf("Successfully routed message from leaf1 to leaf2 via hub: %s", string(receivedMsg))
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for message to be received")
		}
	})
}

func TestMultipleLeafNodes(t *testing.T) {
	t.Run("multiple leaves connect to same hub", func(t *testing.T) {
		// Setup hub cluster
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Setup multiple leaf nodes
		leaf1 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf1)

		leaf2 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf2)

		// Wait for connections to establish
		time.Sleep(2 * time.Second)

		// Verify all nodes are running
		if !hub.nc.server.Running() {
			t.Error("hub cluster is not running")
		}
		if !leaf1.conn.server.Running() {
			t.Error("leaf1 is not running")
		}
		if !leaf2.conn.server.Running() {
			t.Error("leaf2 is not running")
		}

		t.Log("Successfully connected multiple leaf nodes to hub")
	})

	t.Run("bidirectional communication between leaves", func(t *testing.T) {
		// Setup hub cluster
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Setup two leaf nodes
		leaf1 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf1)

		leaf2 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf2)

		// Test bidirectional communication
		subject1to2 := "leaf1.to.leaf2"
		subject2to1 := "leaf2.to.leaf1"

		// Setup channels for received messages
		leaf1Messages := make(chan string, 1)
		leaf2Messages := make(chan string, 1)

		// Subscribe on leaf1 to receive from leaf2
		sub1, err := leaf1.conn.conn.Subscribe(subject2to1, func(msg *nats.Msg) {
			leaf1Messages <- string(msg.Data)
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf1: %v", err)
		}
		defer sub1.Unsubscribe()

		// Subscribe on leaf2 to receive from leaf1
		sub2, err := leaf2.conn.conn.Subscribe(subject1to2, func(msg *nats.Msg) {
			leaf2Messages <- string(msg.Data)
		})
		if err != nil {
			t.Fatalf("failed to subscribe on leaf2: %v", err)
		}
		defer sub2.Unsubscribe()

		// Wait for subscriptions to be ready
		time.Sleep(1 * time.Second)

		// Send message from leaf1 to leaf2
		msg1to2 := "Hello from leaf1"
		err = leaf1.conn.conn.Publish(subject1to2, []byte(msg1to2))
		if err != nil {
			t.Fatalf("failed to publish from leaf1: %v", err)
		}

		// Send message from leaf2 to leaf1
		msg2to1 := "Hello from leaf2"
		err = leaf2.conn.conn.Publish(subject2to1, []byte(msg2to1))
		if err != nil {
			t.Fatalf("failed to publish from leaf2: %v", err)
		}

		// Verify leaf2 received message from leaf1
		select {
		case received := <-leaf2Messages:
			if received != msg1to2 {
				t.Errorf("leaf2 expected '%s', got '%s'", msg1to2, received)
			}
			t.Logf("✓ Leaf2 received: %s", received)
		case <-time.After(5 * time.Second):
			t.Error("timeout: leaf2 did not receive message from leaf1")
		}

		// Verify leaf1 received message from leaf2
		select {
		case received := <-leaf1Messages:
			if received != msg2to1 {
				t.Errorf("leaf1 expected '%s', got '%s'", msg2to1, received)
			}
			t.Logf("✓ Leaf1 received: %s", received)
		case <-time.After(5 * time.Second):
			t.Error("timeout: leaf1 did not receive message from leaf2")
		}

		t.Log("Successfully verified bidirectional communication between leaf nodes via hub")
	})

	t.Run("multiple subjects communication", func(t *testing.T) {
		// Setup hub cluster
		hub := SetupSingleNodeCluster(t)
		defer CleanupClusters(hub)

		// Setup leaf nodes
		leaf1 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf1)

		leaf2 := SetupLeafConnectedToHub(t, hub)
		defer CleanupLeaf(leaf2)

		// Test multiple subjects
		subjects := []string{"orders.created", "orders.updated", "orders.deleted"}
		receivedMessages := make(map[string]string)
		messagesChan := make(chan string, len(subjects))

		// Subscribe to all subjects on leaf2
		for _, subject := range subjects {
			sub, err := leaf2.conn.conn.Subscribe(subject, func(msg *nats.Msg) {
				messagesChan <- fmt.Sprintf("%s:%s", msg.Subject, string(msg.Data))
			})
			if err != nil {
				t.Fatalf("failed to subscribe to %s: %v", subject, err)
			}
			defer sub.Unsubscribe()
		}

		// Wait for subscriptions
		time.Sleep(1 * time.Second)

		// Publish to all subjects from leaf1
		testData := map[string]string{
			"orders.created": "order-123",
			"orders.updated": "order-456",
			"orders.deleted": "order-789",
		}

		for subject, data := range testData {
			err := leaf1.conn.conn.Publish(subject, []byte(data))
			if err != nil {
				t.Fatalf("failed to publish to %s: %v", subject, err)
			}
		}

		// Collect received messages
		timeout := time.After(5 * time.Second)
		for i := 0; i < len(subjects); i++ {
			select {
			case msg := <-messagesChan:
				parts := strings.SplitN(msg, ":", 2)
				if len(parts) == 2 {
					receivedMessages[parts[0]] = parts[1]
				}
			case <-timeout:
				t.Errorf("timeout waiting for message %d", i+1)
			}
		}

		// Verify all messages received correctly
		for subject, expectedData := range testData {
			if received, ok := receivedMessages[subject]; !ok {
				t.Errorf("did not receive message for subject %s", subject)
			} else if received != expectedData {
				t.Errorf("subject %s: expected '%s', got '%s'", subject, expectedData, received)
			} else {
				t.Logf("✓ Subject %s: %s", subject, received)
			}
		}

		t.Log("Successfully verified multiple subjects communication between leaf nodes")
	})
}

func TestLeafNodeConfiguration(t *testing.T) {
	t.Run("leaf options builder pattern", func(t *testing.T) {
		opts := NewLeafOptions("test-leaf").
			WithListen("127.0.0.1", 4301).
			WithLeafAuth("user", "pass").
			WithLeafRemotes([]string{"nats://127.0.0.1:4222"})

		if opts.serverName != "test-leaf" {
			t.Errorf("expected server name 'test-leaf', got '%s'", opts.serverName)
		}

		if opts.host != "127.0.0.1" {
			t.Errorf("expected host '127.0.0.1', got '%s'", opts.host)
		}

		if opts.port != 4301 {
			t.Errorf("expected port 4301, got %d", opts.port)
		}

		if opts.username != "user" {
			t.Errorf("expected username 'user', got '%s'", opts.username)
		}

		if opts.password != "pass" {
			t.Errorf("expected password 'pass', got '%s'", opts.password)
		}

		if len(opts.leafRemotes) != 1 {
			t.Errorf("expected 1 remote, got %d", len(opts.leafRemotes))
		}

		t.Log("Successfully tested leaf options builder pattern")
	})

	t.Run("leaf nats config conversion", func(t *testing.T) {
		opts := NewLeafOptions("config-test").
			WithListen("0.0.0.0", 4302).
			WithLeafAuth("testuser", "testpass").
			WithLeafRemotes([]string{"nats://hub1:4222"}, []string{"nats://hub2:4222"})

		natsConfig := opts.toNATSConfig()

		if natsConfig.ServerName != "config-test" {
			t.Errorf("expected server name 'config-test', got '%s'", natsConfig.ServerName)
		}

		if natsConfig.Host != "0.0.0.0" {
			t.Errorf("expected host '0.0.0.0', got '%s'", natsConfig.Host)
		}

		if natsConfig.Port != 4302 {
			t.Errorf("expected port 4302, got %d", natsConfig.Port)
		}

		if natsConfig.LeafNode.Username != "testuser" {
			t.Errorf("expected username 'testuser', got '%s'", natsConfig.LeafNode.Username)
		}

		if natsConfig.LeafNode.Password != "testpass" {
			t.Errorf("expected password 'testpass', got '%s'", natsConfig.LeafNode.Password)
		}

		if len(natsConfig.LeafNode.Remotes) != 2 {
			t.Errorf("expected 2 remotes, got %d", len(natsConfig.LeafNode.Remotes))
		}

		t.Log("Successfully tested NATS config conversion")
	})
}

func TestLeafNodeEdgeCases(t *testing.T) {
	t.Run("leaf with no remotes", func(t *testing.T) {
		config := DefaultLeafTestConfig("standalone-leaf", 0)

		leaf, err := config.CreateLeaf()
		if err != nil {
			t.Fatalf("failed to create standalone leaf: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 5*time.Second)

		// Verify leaf can run without remotes
		if !leaf.conn.server.Running() {
			t.Error("standalone leaf is not running")
		}

		t.Log("Successfully created standalone leaf without remotes")
	})

	t.Run("leaf with invalid remote URL", func(t *testing.T) {
		config := DefaultLeafTestConfig("invalid-remote-leaf", 0).
			WithRemotes([]string{"invalid-url"})

		leaf, err := config.CreateLeaf()
		if err != nil {
			t.Fatalf("failed to create leaf with invalid remote: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 5*time.Second)

		// Leaf should still start even with invalid remote URLs
		if !leaf.conn.server.Running() {
			t.Error("leaf with invalid remote is not running")
		}

		t.Log("Successfully handled leaf with invalid remote URL")
	})
}

// TestLeafNodeJetStream tests JetStream functionality in leaf nodes
func TestLeafNodeJetStream(t *testing.T) {
	t.Run("leaf with jetstream enabled", func(t *testing.T) {
		tempDir := t.TempDir()

		opts := NewLeafOptions("jetstream-leaf").
			WithListen("127.0.0.1", 4301).
			WithJetStream(true).
			WithStoreDir(tempDir)

		leaf, err := NewLeaf(opts)
		if err != nil {
			t.Fatalf("failed to create leaf with JetStream: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 10*time.Second)

		// Verify JetStream is enabled
		if !leaf.conn.server.Running() {
			t.Error("leaf with JetStream is not running")
		}

		// Check if JetStream context is available
		if leaf.conn.js == nil {
			t.Error("JetStream context is not available")
		}

		// Try to get JetStream account info
		info, err := leaf.conn.js.AccountInfo()
		if err != nil {
			t.Logf("JetStream AccountInfo error (expected for leaf node): %v", err)
		} else {
			t.Logf("JetStream AccountInfo: %+v", info)
		}

		t.Log("Successfully created leaf node with JetStream enabled")
	})

	t.Run("leaf with jetstream configuration", func(t *testing.T) {
		tempDir := t.TempDir()

		opts := NewLeafOptions("configured-jetstream-leaf").
			WithListen("127.0.0.1", 4302).
			WithJetStream(true).
			WithStoreDir(tempDir).
			WithJetStreamMaxMemory(size.NewSizeFromMegabytes(64)).
			WithJetStreamMaxStore(size.NewSizeFromMegabytes(128))

		leaf, err := NewLeaf(opts)
		if err != nil {
			t.Fatalf("failed to create configured leaf with JetStream: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 10*time.Second)

		// Verify the leaf is running
		if !leaf.conn.server.Running() {
			t.Error("configured leaf with JetStream is not running")
		}

		// Verify JetStream context
		if leaf.conn.js == nil {
			t.Error("JetStream context is not available in configured leaf")
		}

		t.Log("Successfully created leaf node with JetStream configuration")
	})

	t.Run("leaf without jetstream", func(t *testing.T) {
		opts := NewLeafOptions("no-jetstream-leaf").
			WithListen("127.0.0.1", 4303).
			WithJetStream(false)

		leaf, err := NewLeaf(opts)
		if err != nil {
			t.Fatalf("failed to create leaf without JetStream: %v", err)
		}
		defer CleanupLeaf(leaf)

		waitForLeafReady(t, leaf, 5*time.Second)

		// Verify the leaf is running
		if !leaf.conn.server.Running() {
			t.Error("leaf without JetStream is not running")
		}

		t.Log("Successfully created leaf node without JetStream")
	})
}
