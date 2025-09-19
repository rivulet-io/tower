package mesh

import (
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestCoreNATSPublishVolatile(t *testing.T) {
	t.Run("basic publish", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.publish"
		message := []byte("hello world")

		// Publish from node1
		err := cluster1.nc.PublishVolatile(subject, message)
		if err != nil {
			t.Fatalf("failed to publish message: %v", err)
		}

		t.Log("Successfully published volatile message")
	})

	t.Run("publish with headers", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.publish.headers"
		message := []byte("hello with headers")
		headers := make(nats.Header)
		headers.Set("Content-Type", "application/json")
		headers.Set("User-ID", "12345")

		// Publish from node1 with headers
		err := cluster1.nc.PublishVolatile(subject, message, headers)
		if err != nil {
			t.Fatalf("failed to publish message with headers: %v", err)
		}

		t.Log("Successfully published volatile message with headers")
	})
}

func TestCoreNATSSubscribeVolatileFanout(t *testing.T) {
	t.Run("fanout subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.fanout"
		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Subscribe on node2
		cancel, err := cluster2.nc.SubscribeVolatileViaFanout(
			subject,
			func(subj string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, string(msg))
				mu.Unlock()
				wg.Done()
				return []byte("response"), nil, false // No reply needed for this test
			},
			func(err error) {
				t.Logf("Error in fanout handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}
		defer cancel()

		// Give subscription time to be established
		time.Sleep(100 * time.Millisecond)

		// Publish messages from node1
		messages := []string{"message1", "message2", "message3"}
		wg.Add(len(messages))

		for _, msg := range messages {
			err := cluster1.nc.PublishVolatile(subject, []byte(msg))
			if err != nil {
				t.Fatalf("failed to publish message %s: %v", msg, err)
			}
		}

		// Wait for all messages to be received
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(messages) {
			t.Errorf("expected %d messages, got %d", len(messages), len(receivedMessages))
		}
		mu.Unlock()

		t.Logf("Successfully received %d fanout messages", len(receivedMessages))
	})
}

func TestCoreNATSSubscribeVolatileQueue(t *testing.T) {
	t.Run("queue subscription", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.queue"
		queueGroup := "workers"
		receivedMessages := make([]string, 0)
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Subscribe on multiple nodes with same queue group
		cancel1, err := cluster2.nc.SubscribeVolatileViaQueue(
			subject,
			queueGroup,
			func(subj string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, "node2:"+string(msg))
				mu.Unlock()
				wg.Done()
				return nil, nil, false
			},
			func(err error) {
				t.Logf("Error in queue handler node2: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe on node2: %v", err)
		}
		defer cancel1()

		cancel2, err := cluster3.nc.SubscribeVolatileViaQueue(
			subject,
			queueGroup,
			func(subj string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool) {
				mu.Lock()
				receivedMessages = append(receivedMessages, "node3:"+string(msg))
				mu.Unlock()
				wg.Done()
				return nil, nil, false
			},
			func(err error) {
				t.Logf("Error in queue handler node3: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe on node3: %v", err)
		}
		defer cancel2()

		// Give subscriptions time to be established
		time.Sleep(100 * time.Millisecond)

		// Publish messages from node1
		messages := []string{"task1", "task2", "task3", "task4", "task5"}
		wg.Add(len(messages))

		for _, msg := range messages {
			err := cluster1.nc.PublishVolatile(subject, []byte(msg))
			if err != nil {
				t.Fatalf("failed to publish message %s: %v", msg, err)
			}
		}

		// Wait for all messages to be received
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for messages")
		}

		mu.Lock()
		if len(receivedMessages) != len(messages) {
			t.Errorf("expected %d messages, got %d", len(messages), len(receivedMessages))
		}

		// Check that messages were distributed between nodes
		node2Count := 0
		node3Count := 0
		for _, msg := range receivedMessages {
			if len(msg) > 5 && msg[:5] == "node2" {
				node2Count++
			} else if len(msg) > 5 && msg[:5] == "node3" {
				node3Count++
			}
		}

		if node2Count == 0 && node3Count == 0 {
			t.Error("no messages were processed by queue subscribers")
		}
		mu.Unlock()

		t.Logf("Successfully distributed %d messages across queue group (node2: %d, node3: %d)",
			len(receivedMessages), node2Count, node3Count)
	})
}

func TestCoreNATSRequestVolatile(t *testing.T) {
	t.Run("request-response", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.request"

		// Set up responder on node2
		cancel, err := cluster2.nc.SubscribeVolatileViaFanout(
			subject,
			func(subj string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool) {
				response := []byte("response to: " + string(msg))
				responseHeaders := make(nats.Header)
				responseHeaders.Set("Response-From", "node2")
				return response, responseHeaders, true // Send reply
			},
			func(err error) {
				t.Logf("Error in responder: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to set up responder: %v", err)
		}
		defer cancel()

		// Give subscription time to be established
		time.Sleep(100 * time.Millisecond)

		// Send request from node1
		requestMsg := []byte("ping")
		requestHeaders := make(nats.Header)
		requestHeaders.Set("Request-ID", "12345")

		responseData, responseHeaders, err := cluster1.nc.RequestVolatile(
			subject,
			requestMsg,
			5*time.Second,
			requestHeaders,
		)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		expectedResponse := "response to: ping"
		if string(responseData) != expectedResponse {
			t.Errorf("expected response %q, got %q", expectedResponse, string(responseData))
		}

		if responseHeaders.Get("Response-From") != "node2" {
			t.Errorf("expected Response-From header to be 'node2', got %q", responseHeaders.Get("Response-From"))
		}

		t.Logf("Successfully completed request-response: %s -> %s", string(requestMsg), string(responseData))
	})
}

func TestCoreNATSPublishVolatileBatch(t *testing.T) {
	t.Run("batch publish", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		subject := "test.batch"
		receivedCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Subscribe on node2
		cancel, err := cluster2.nc.SubscribeVolatileViaFanout(
			subject,
			func(subj string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool) {
				mu.Lock()
				receivedCount++
				mu.Unlock()
				wg.Done()
				return nil, nil, false
			},
			func(err error) {
				t.Logf("Error in batch handler: %v", err)
			},
		)
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}
		defer cancel()

		// Give subscription time to be established
		time.Sleep(100 * time.Millisecond)

		// Prepare batch messages
		batchMessages := []struct {
			Subject string
			Data    []byte
			Headers nats.Header
		}{
			{
				Subject: subject,
				Data:    []byte("batch message 1"),
				Headers: nats.Header{"Batch-ID": []string{"1"}},
			},
			{
				Subject: subject,
				Data:    []byte("batch message 2"),
				Headers: nats.Header{"Batch-ID": []string{"2"}},
			},
			{
				Subject: subject,
				Data:    []byte("batch message 3"),
				Headers: nats.Header{"Batch-ID": []string{"3"}},
			},
		}

		wg.Add(len(batchMessages))

		// Publish batch from node1
		err = cluster1.nc.PublishVolatileBatch(batchMessages)
		if err != nil {
			t.Fatalf("failed to publish batch: %v", err)
		}

		// Wait for all messages to be received
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			// Success
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for batch messages")
		}

		mu.Lock()
		if receivedCount != len(batchMessages) {
			t.Errorf("expected %d messages, got %d", len(batchMessages), receivedCount)
		}
		mu.Unlock()

		t.Logf("Successfully published and received %d batch messages", receivedCount)
	})
}

func TestCoreNATSFlushTimeout(t *testing.T) {
	t.Run("flush timeout", func(t *testing.T) {
		cluster1, _, _ := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1)

		// Test flush with timeout
		err := cluster1.nc.FlushTimeout(1 * time.Second)
		if err != nil {
			t.Fatalf("failed to flush with timeout: %v", err)
		}

		t.Log("Successfully flushed connection with timeout")
	})
}
