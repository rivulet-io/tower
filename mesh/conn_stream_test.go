package mesh

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamCreateOrUpdateStream(t *testing.T) {
	t.Run("create stream", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream configuration
		config := &PersistentConfig{
			Name:        "orders_",
			Description: "Test stream for orders",
			Subjects:    []string{"orders.shop", "payments.shop"},
			Retention:   nats.WorkQueuePolicy,
			MaxMsgs:     1000,
			MaxBytes:    1024 * 1024, // 1MB
			MaxAge:      24 * time.Hour,
			Replicas:    1,
			Metadata: map[string]string{
				"team":    "backend",
				"service": "orders",
			},
		}

		// Create stream on node1
		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// Verify stream exists by getting info
		streamName := "orders_"
		info, err := cluster1.nc.GetStreamInfo(streamName)
		if err != nil {
			t.Fatalf("failed to get stream info: %v", err)
		}

		if info.Config.Name != streamName {
			t.Errorf("expected stream name %q, got %q", streamName, info.Config.Name)
		}

		if len(info.Config.Subjects) != 2 {
			t.Errorf("expected 2 subjects, got %d", len(info.Config.Subjects))
		}

		t.Logf("Successfully created stream: %s with %d subjects", info.Config.Name, len(info.Config.Subjects))
	})

	t.Run("update existing stream", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create initial stream
		config := &PersistentConfig{
			Name:        "test_",
			Description: "Initial stream",
			Subjects:    []string{"test.log"},
			MaxMsgs:     500,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create initial stream: %v", err)
		}

		// Update stream with new configuration
		config.Description = "Updated stream"
		config.MaxMsgs = 1000

		err = cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to update stream: %v", err)
		}

		// Verify update
		info, err := cluster1.nc.GetStreamInfo("test_")
		if err != nil {
			t.Fatalf("failed to get updated stream info: %v", err)
		}

		if info.Config.Description != "Updated stream" {
			t.Errorf("expected description 'Updated stream', got %q", info.Config.Description)
		}

		if info.Config.MaxMsgs != 1000 {
			t.Errorf("expected MaxMsgs 1000, got %d", info.Config.MaxMsgs)
		}

		t.Log("Successfully updated stream configuration")
	})
}

func TestJetStreamPublishPersistent(t *testing.T) {
	t.Run("basic publish", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream first
		config := &PersistentConfig{
			Name:     "events_",
			Subjects: []string{"events.user.created", "events.user.updated", "events.user.deleted"},
			MaxMsgs:  100,
			Replicas: 3, // Use 3 replicas for cluster
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// Wait for stream to be ready and fully replicated across cluster
		time.Sleep(2 * time.Second)

		// Verify stream is accessible
		streamName := "events.user.created"
		info, err := cluster1.nc.GetStreamInfo(streamName)
		if err != nil {
			t.Fatalf("stream not accessible after creation: %v", err)
		}
		t.Logf("Stream %s is ready", streamName)
		t.Logf("Stream subjects: %v", info.Config.Subjects)
		t.Logf("Stream storage: %v", info.Config.Storage)
		t.Logf("Stream state - messages: %d", info.State.Msgs)

		// Try to publish the first message (this should work)
		subject := "events.user.created"
		message := []byte("test event data")

		t.Logf("Publishing to subject: %s", subject)

		err = cluster1.nc.PublishPersistent(subject, message)
		if err != nil {
			// Try with a different node
			t.Logf("Failed with node1, trying node2: %v", err)
			err = cluster2.nc.PublishPersistent(subject, message)
			if err != nil {
				// Try with node3
				t.Logf("Failed with node2, trying node3: %v", err)
				err = cluster3.nc.PublishPersistent(subject, message)
				if err != nil {
					t.Fatalf("failed to publish to %s: %v", subject, err)
				} else {
					t.Log("Successfully published with node3")
				}
			} else {
				t.Log("Successfully published with node2")
			}
		} else {
			t.Log("Successfully published with node1")
		}

		// Publish messages
		subjects := []string{"events.user.created", "events.user.updated", "events.user.deleted"}
		messages := []string{"user-123-created", "user-123-updated", "user-123-deleted"}

		for i, subject := range subjects {
			err := cluster1.nc.PublishPersistent(subject, []byte(messages[i]))
			if err != nil {
				t.Logf("Failed to publish with node1: %v, trying node2", err)
				err = cluster2.nc.PublishPersistent(subject, []byte(messages[i]))
				if err != nil {
					t.Fatalf("failed to publish to %s: %v", subject, err)
				}
			}
		}

		// Verify messages are stored
		info, err = cluster1.nc.GetStreamInfo("events_")
		if err != nil {
			t.Fatalf("failed to get stream info: %v", err)
		}

		if info.State.Msgs != uint64(len(messages)) {
			t.Errorf("expected %d messages in stream, got %d", len(messages), info.State.Msgs)
		}

		t.Logf("Successfully published %d persistent messages", len(messages))
	})

	t.Run("publish with options", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Name:     "orders_",
			Subjects: []string{"orders.new"},
			MaxMsgs:  100,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// Publish with options
		subject := "orders.new"
		message := []byte(`{"orderId": "order-123", "amount": 99.99}`)

		ack, err := cluster1.nc.PublishPersistentWithOptions(
			subject,
			message,
			nats.ExpectStream("orders_"),
			nats.MsgId("order-123"),
		)
		if err != nil {
			t.Fatalf("failed to publish with options: %v", err)
		}

		if ack.Stream != "orders_" {
			t.Errorf("expected stream 'orders_', got %q", ack.Stream)
		}

		if ack.Sequence != 1 {
			t.Errorf("expected sequence 1, got %d", ack.Sequence)
		}

		t.Logf("Successfully published with options: stream=%s, seq=%d", ack.Stream, ack.Sequence)
	})
}

func TestJetStreamSubscribeStreamViaDurable(t *testing.T) {
	t.Run("durable subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Subjects: []string{"notifications.*"},
			MaxMsgs:  100,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Subscribe with durable consumer on node2
		cancel, err := cluster2.nc.SubscribeStreamViaDurable(
			"notification-processor",
			"notifications.*",
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
				wg.Done()
				return nil, false, true // Acknowledge message
			},
			func(err error) {
				t.Logf("Error in durable handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}
		defer cancel()

		// Give subscription time to be established
		time.Sleep(100 * time.Millisecond)

		// Publish messages from node1
		messages := []string{"email-notification", "push-notification", "sms-notification"}
		wg.Add(len(messages))

		for i, msg := range messages {
			subject := "notifications.email"
			if i == 1 {
				subject = "notifications.push"
			} else if i == 2 {
				subject = "notifications.sms"
			}

			err := cluster1.nc.PublishPersistent(subject, []byte(msg))
			if err != nil {
				t.Fatalf("failed to publish message %s: %v", msg, err)
			}
		}

		// Wait for messages
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for durable subscription messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(messages) {
			t.Errorf("expected %d messages, got %d", len(messages), len(receivedMessages))
		}
		mu.Unlock()

		t.Logf("Successfully received %d messages via durable subscription", len(receivedMessages))
	})
}

func TestJetStreamPullPersistentViaDurable(t *testing.T) {
	t.Run("pull subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Subjects: []string{"tasks.*"},
			MaxMsgs:  100,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// First, publish some messages
		tasks := []string{"task-1", "task-2", "task-3", "task-4", "task-5"}
		for _, task := range tasks {
			err := cluster1.nc.PublishPersistent("tasks.process", []byte(task))
			if err != nil {
				t.Fatalf("failed to publish task %s: %v", task, err)
			}
		}

		// Now set up pull consumer
		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		pullOptions := PullOptions{
			Batch:    2,
			MaxWait:  5 * time.Second,
			Interval: 100 * time.Millisecond,
		}

		wg.Add(len(tasks))

		cancel, err := cluster2.nc.PullPersistentViaDurable(
			"task-processor",
			"tasks.*",
			pullOptions,
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
				wg.Done()
				return nil, false, true // Acknowledge message
			},
			func(err error) {
				t.Logf("Error in pull handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to set up pull consumer: %v", err)
		}
		defer cancel()

		// Wait for messages
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for pull subscription messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(tasks) {
			t.Errorf("expected %d messages, got %d", len(tasks), len(receivedMessages))
		}
		mu.Unlock()

		t.Logf("Successfully pulled %d messages with batch size %d", len(receivedMessages), pullOptions.Batch)
	})
}

func TestJetStreamSubscribePersistentViaEphemeral(t *testing.T) {
	t.Run("ephemeral subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Subjects: []string{"logs.*"},
			MaxMsgs:  100,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Subscribe with ephemeral consumer on node3
		cancel, err := cluster3.nc.SubscribePersistentViaEphemeral(
			"logs.*",
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
				wg.Done()
				return nil, false, true // Acknowledge message
			},
			func(err error) {
				t.Logf("Error in ephemeral handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}
		defer cancel()

		// Give subscription time to be established
		time.Sleep(100 * time.Millisecond)

		// Publish messages from node1
		logs := []string{"INFO: Service started", "WARN: High memory usage", "ERROR: Database connection failed"}
		wg.Add(len(logs))

		for i, log := range logs {
			subject := "logs.info"
			if i == 1 {
				subject = "logs.warn"
			} else if i == 2 {
				subject = "logs.error"
			}

			err := cluster1.nc.PublishPersistent(subject, []byte(log))
			if err != nil {
				t.Fatalf("failed to publish log %s: %v", log, err)
			}
		}

		// Wait for messages
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for ephemeral subscription messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(logs) {
			t.Errorf("expected %d messages, got %d", len(logs), len(receivedMessages))
		}
		mu.Unlock()

		t.Logf("Successfully received %d messages via ephemeral subscription", len(receivedMessages))
	})
}

func TestJetStreamPullPersistentViaEphemeral(t *testing.T) {
	t.Run("ephemeral pull subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Subjects: []string{"metrics.*"},
			MaxMsgs:  100,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// Publish metrics
		metrics := []string{"cpu-usage-80", "memory-usage-90", "disk-usage-70"}
		for _, metric := range metrics {
			err := cluster1.nc.PublishPersistent("metrics.system", []byte(metric))
			if err != nil {
				t.Fatalf("failed to publish metric %s: %v", metric, err)
			}
		}

		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		pullOptions := PullOptions{
			Batch:    1,
			MaxWait:  3 * time.Second,
			Interval: 50 * time.Millisecond,
		}

		wg.Add(len(metrics))

		cancel, err := cluster3.nc.PullPersistentViaEphemeral(
			"metrics.*",
			pullOptions,
			func(subject string, msg []byte) (response []byte, reply bool, ack bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
				wg.Done()
				return nil, false, true // Acknowledge message
			},
			func(err error) {
				t.Logf("Error in ephemeral pull handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to set up ephemeral pull consumer: %v", err)
		}
		defer cancel()

		// Wait for messages
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for ephemeral pull subscription messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(metrics) {
			t.Errorf("expected %d messages, got %d", len(metrics), len(receivedMessages))
		}
		mu.Unlock()

		t.Logf("Successfully pulled %d messages via ephemeral consumer", len(receivedMessages))
	})
}

func TestJetStreamDeleteStream(t *testing.T) {
	t.Run("delete stream", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create stream
		config := &PersistentConfig{
			Subjects: []string{"temp.*"},
			MaxMsgs:  10,
		}

		err := cluster1.nc.CreateOrUpdateStream(config)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		streamName := "temp_"

		// Verify stream exists
		_, err = cluster1.nc.GetStreamInfo(streamName)
		if err != nil {
			t.Fatalf("stream should exist before deletion: %v", err)
		}

		// Delete stream
		err = cluster1.nc.DeleteStream(streamName)
		if err != nil {
			t.Fatalf("failed to delete stream: %v", err)
		}

		// Verify stream is deleted
		_, err = cluster1.nc.GetStreamInfo(streamName)
		if err == nil {
			t.Error("stream should not exist after deletion")
		}

		t.Log("Successfully deleted stream")
	})
}
