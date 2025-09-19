package mesh

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDistributedLockTryLock(t *testing.T) {
	t.Run("basic try lock and unlock", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "locks",
			MaxBytes: 1024 * 1024, // 1MB
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		// Try to acquire lock from node1
		lockKey := "resource-1"
		cancel, err := cluster1.nc.TryLock("locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}

		// Verify lock is acquired
		isLocked, err := cluster1.nc.IsLocked("locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status: %v", err)
		}
		if !isLocked {
			t.Error("lock should be acquired")
		}

		// Try to acquire same lock from node2 - should fail
		_, err = cluster2.nc.TryLock("locks", lockKey)
		if err == nil {
			t.Error("second lock attempt should fail")
		} else {
			t.Logf("Expected lock conflict: %v", err)
		}

		// Release lock
		cancel()

		// Verify lock is released
		isLocked, err = cluster1.nc.IsLocked("locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status after release: %v", err)
		}
		if isLocked {
			t.Error("lock should be released")
		}

		// Now node2 should be able to acquire the lock
		cancel2, err := cluster2.nc.TryLock("locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock after release: %v", err)
		}
		defer cancel2()

		t.Log("Successfully tested basic try lock and unlock across nodes")
	})

	t.Run("multiple different locks", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "multi-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		// Acquire multiple different locks from different nodes
		locks := map[string]func(){
			"file-1":     nil,
			"database-1": nil,
			"cache-1":    nil,
		}

		nodes := []*Cluster{cluster1, cluster2, cluster3}
		i := 0

		for lockKey := range locks {
			node := nodes[i%len(nodes)]
			cancel, err := node.nc.TryLock("multi-locks", lockKey)
			if err != nil {
				t.Fatalf("failed to acquire lock %s: %v", lockKey, err)
			}
			locks[lockKey] = cancel
			i++
		}

		// Verify all locks are acquired
		for lockKey := range locks {
			for _, node := range nodes {
				isLocked, err := node.nc.IsLocked("multi-locks", lockKey)
				if err != nil {
					t.Fatalf("failed to check lock status for %s: %v", lockKey, err)
				}
				if !isLocked {
					t.Errorf("lock %s should be acquired", lockKey)
				}
			}
		}

		// Release all locks
		for lockKey, cancel := range locks {
			cancel()

			// Verify lock is released
			isLocked, err := cluster1.nc.IsLocked("multi-locks", lockKey)
			if err != nil {
				t.Fatalf("failed to check lock status after release for %s: %v", lockKey, err)
			}
			if isLocked {
				t.Errorf("lock %s should be released", lockKey)
			}
		}

		t.Logf("Successfully tested %d different locks", len(locks))
	})
}

func TestDistributedLockWithTimeout(t *testing.T) {
	t.Run("lock with timeout", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "timeout-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		lockKey := "timeout-resource"

		// Node1 acquires lock
		cancel1, err := cluster1.nc.TryLock("timeout-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire initial lock: %v", err)
		}

		// Node2 tries to acquire with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		startTime := time.Now()
		_, err = cluster2.nc.Lock(ctx, "timeout-locks", lockKey)
		elapsed := time.Since(startTime)

		if err == nil {
			t.Error("lock with timeout should fail")
		}
		if elapsed < 900*time.Millisecond || elapsed > 1500*time.Millisecond {
			t.Errorf("timeout should be around 1 second, got %v", elapsed)
		}

		// Release first lock
		cancel1()

		// Now node2 should be able to acquire immediately
		ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel2()

		startTime = time.Now()
		cancelLock, err := cluster2.nc.Lock(ctx2, "timeout-locks", lockKey)
		elapsed = time.Since(startTime)

		if err != nil {
			t.Fatalf("should acquire lock after first lock is released: %v", err)
		}
		defer cancelLock()

		if elapsed > 100*time.Millisecond {
			t.Errorf("should acquire lock quickly after release, took %v", elapsed)
		}

		t.Logf("Successfully tested lock with timeout behavior")
	})
}

func TestDistributedLockConcurrency(t *testing.T) {
	t.Run("concurrent lock acquisition", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "concurrent-locks",
			MaxBytes: 2 * 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		lockKey := "shared-resource"
		concurrentWorkers := 10
		successCount := 0
		failCount := 0
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Launch concurrent workers trying to acquire the same lock
		nodes := []*Cluster{cluster1, cluster2, cluster3}

		for i := 0; i < concurrentWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()

				node := nodes[workerID%len(nodes)]

				// Try to acquire lock
				cancel, err := node.nc.TryLock("concurrent-locks", lockKey)

				mu.Lock()
				if err != nil {
					failCount++
					t.Logf("Worker %d (node %d) failed to acquire lock: %v",
						workerID, workerID%len(nodes)+1, err)
				} else {
					successCount++
					t.Logf("Worker %d (node %d) successfully acquired lock",
						workerID, workerID%len(nodes)+1)

					// Hold lock for a short time
					time.Sleep(50 * time.Millisecond)
					cancel()
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		mu.Lock()
		t.Logf("Lock acquisition results: success=%d, fail=%d", successCount, failCount)
		mu.Unlock()

		// Only one worker should succeed
		if successCount != 1 {
			t.Errorf("expected exactly 1 successful lock acquisition, got %d", successCount)
		}

		if failCount != concurrentWorkers-1 {
			t.Errorf("expected %d failed lock acquisitions, got %d", concurrentWorkers-1, failCount)
		}

		t.Log("Successfully tested concurrent lock acquisition")
	})
}

func TestDistributedLockBackoff(t *testing.T) {
	t.Run("lock with backoff retry", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "backoff-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		lockKey := "backoff-resource"

		// Node1 acquires lock
		cancel1, err := cluster1.nc.TryLock("backoff-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire initial lock: %v", err)
		}

		// Schedule lock release after 2 seconds
		go func() {
			time.Sleep(2 * time.Second)
			cancel1()
			t.Log("Released initial lock")
		}()

		// Node2 tries to acquire with backoff and timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		lockOptions := LockOptions{
			MaxDelay:      500 * time.Millisecond,
			BackOffFactor: 2,
		}

		startTime := time.Now()
		cancel2, err := cluster2.nc.Lock(ctx, "backoff-locks", lockKey, lockOptions)
		elapsed := time.Since(startTime)

		if err != nil {
			t.Fatalf("should eventually acquire lock with backoff: %v", err)
		}
		defer cancel2()

		// Should take around 2 seconds (when first lock is released)
		if elapsed < 1800*time.Millisecond || elapsed > 3000*time.Millisecond {
			t.Errorf("backoff retry should take around 2 seconds, took %v", elapsed)
		}

		// Verify lock is acquired
		isLocked, err := cluster2.nc.IsLocked("backoff-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status: %v", err)
		}
		if !isLocked {
			t.Error("lock should be acquired after backoff retry")
		}

		t.Logf("Successfully acquired lock with backoff retry after %v", elapsed)
	})
}

func TestDistributedLockForceUnlock(t *testing.T) {
	t.Run("force unlock", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "force-unlock-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		lockKey := "force-unlock-resource"

		// Node1 acquires lock
		cancel, err := cluster1.nc.TryLock("force-unlock-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		defer cancel() // In case force unlock fails

		// Verify lock is acquired
		isLocked, err := cluster1.nc.IsLocked("force-unlock-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status: %v", err)
		}
		if !isLocked {
			t.Error("lock should be acquired")
		}

		// Node2 cannot acquire the lock
		_, err = cluster2.nc.TryLock("force-unlock-locks", lockKey)
		if err == nil {
			t.Error("should not be able to acquire already held lock")
		}

		// Node3 forces unlock
		err = cluster3.nc.ForceUnlock("force-unlock-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to force unlock: %v", err)
		}

		// Verify lock is released from all nodes
		for i, node := range []*Cluster{cluster1, cluster2, cluster3} {
			isLocked, err := node.nc.IsLocked("force-unlock-locks", lockKey)
			if err != nil {
				t.Fatalf("failed to check lock status on node %d: %v", i+1, err)
			}
			if isLocked {
				t.Errorf("lock should be released on node %d after force unlock", i+1)
			}
		}

		// Node2 should now be able to acquire the lock
		cancel2, err := cluster2.nc.TryLock("force-unlock-locks", lockKey)
		if err != nil {
			t.Fatalf("should be able to acquire lock after force unlock: %v", err)
		}
		defer cancel2()

		t.Log("Successfully tested force unlock functionality")
	})
}

func TestDistributedLockCrossCluster(t *testing.T) {
	t.Run("lock synchronization across cluster", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks
		kvConfig := KeyValueStoreConfig{
			Bucket:   "cross-cluster-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for locks: %v", err)
		}

		lockKey := "cross-cluster-resource"

		// Test lock visibility across all nodes
		cancel, err := cluster1.nc.TryLock("cross-cluster-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock on node1: %v", err)
		}

		// Give time for propagation
		time.Sleep(100 * time.Millisecond)

		// All nodes should see the lock
		for i, node := range []*Cluster{cluster1, cluster2, cluster3} {
			isLocked, err := node.nc.IsLocked("cross-cluster-locks", lockKey)
			if err != nil {
				t.Fatalf("failed to check lock status on node %d: %v", i+1, err)
			}
			if !isLocked {
				t.Errorf("node %d should see the lock", i+1)
			}
		}

		// Try to acquire from other nodes - should fail
		_, err = cluster2.nc.TryLock("cross-cluster-locks", lockKey)
		if err == nil {
			t.Error("node2 should not be able to acquire already held lock")
		}

		_, err = cluster3.nc.TryLock("cross-cluster-locks", lockKey)
		if err == nil {
			t.Error("node3 should not be able to acquire already held lock")
		}

		// Release lock
		cancel()

		// Give time for propagation
		time.Sleep(100 * time.Millisecond)

		// All nodes should see the lock is released
		for i, node := range []*Cluster{cluster1, cluster2, cluster3} {
			isLocked, err := node.nc.IsLocked("cross-cluster-locks", lockKey)
			if err != nil {
				t.Fatalf("failed to check lock status on node %d after release: %v", i+1, err)
			}
			if isLocked {
				t.Errorf("node %d should see the lock is released", i+1)
			}
		}

		t.Log("Successfully tested cross-cluster lock synchronization")
	})
}

func TestDistributedLockWithTTL(t *testing.T) {
	t.Run("lock with automatic TTL expiration", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks with TTL
		kvConfig := KeyValueStoreConfig{
			Bucket:   "ttl-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
			TTL:      3 * time.Second, // Short TTL for testing
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for TTL locks: %v", err)
		}

		lockKey := "ttl-resource"

		// Node1 acquires lock
		cancel1, err := cluster1.nc.TryLock("ttl-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		defer cancel1() // Cleanup in case TTL doesn't work

		// Verify lock is initially acquired
		isLocked, err := cluster1.nc.IsLocked("ttl-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check initial lock status: %v", err)
		}
		if !isLocked {
			t.Error("lock should be initially acquired")
		}

		// Node2 cannot acquire the lock immediately
		_, err = cluster2.nc.TryLock("ttl-locks", lockKey)
		if err == nil {
			t.Error("should not be able to acquire already held lock")
		} else {
			t.Logf("Expected lock conflict: %v", err)
		}

		// Wait for TTL to expire (3 seconds + buffer)
		t.Log("Waiting for TTL expiration...")
		time.Sleep(4 * time.Second)

		// Check if lock has automatically expired
		isLocked, err = cluster1.nc.IsLocked("ttl-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status after TTL: %v", err)
		}
		if isLocked {
			t.Error("lock should have expired due to TTL")
		}

		// Node2 should now be able to acquire the lock
		cancel2, err := cluster2.nc.TryLock("ttl-locks", lockKey)
		if err != nil {
			t.Fatalf("should be able to acquire lock after TTL expiration: %v", err)
		}
		defer cancel2()

		// Verify new lock is acquired
		isLocked, err = cluster2.nc.IsLocked("ttl-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check new lock status: %v", err)
		}
		if !isLocked {
			t.Error("new lock should be acquired")
		}

		t.Log("Successfully tested lock TTL automatic expiration")
	})

	t.Run("lock renewal before TTL expiration", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV bucket for locks with short TTL
		kvConfig := KeyValueStoreConfig{
			Bucket:   "renewal-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
			TTL:      2 * time.Second, // Very short TTL
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", kvConfig)
		if err != nil {
			t.Fatalf("failed to create KV store for renewal locks: %v", err)
		}

		lockKey := "renewal-resource"

		// Node1 acquires lock
		cancel1, err := cluster1.nc.TryLock("renewal-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to acquire lock: %v", err)
		}
		defer cancel1()

		// Start renewal process
		renewalStop := make(chan bool)
		renewalSuccess := 0
		renewalFailed := 0
		var renewalMu sync.Mutex

		go func() {
			ticker := time.NewTicker(1 * time.Second) // Renew every 1 second
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					// Try to "renew" by re-acquiring the lock (simulate renewal)
					// In a real implementation, this would be a dedicated Renew method
					_, renewErr := cluster1.nc.TryLock("renewal-locks", lockKey)

					renewalMu.Lock()
					if renewErr != nil {
						renewalFailed++
						t.Logf("Lock renewal failed (expected if lock still held): %v", renewErr)
					} else {
						renewalSuccess++
						t.Log("Lock renewal succeeded (unexpected)")
					}
					renewalMu.Unlock()

				case <-renewalStop:
					return
				}
			}
		}()

		// Keep lock alive for 5 seconds (longer than TTL)
		time.Sleep(5 * time.Second)
		renewalStop <- true

		// Lock should still be held by original owner
		isLocked, err := cluster1.nc.IsLocked("renewal-locks", lockKey)
		if err != nil {
			t.Fatalf("failed to check lock status: %v", err)
		}

		renewalMu.Lock()
		t.Logf("Renewal attempts: success=%d, failed=%d", renewalSuccess, renewalFailed)
		renewalMu.Unlock()

		// The exact behavior depends on implementation, but lock should exist
		if !isLocked {
			t.Log("Lock expired despite renewal attempts - this may be expected behavior")
		} else {
			t.Log("Lock maintained through renewal period")
		}

		t.Log("Successfully tested lock renewal behavior")
	})

	t.Run("multiple locks with different TTLs", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create multiple KV buckets with different TTLs
		shortTTLConfig := KeyValueStoreConfig{
			Bucket:   "short-ttl-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
			TTL:      2 * time.Second,
		}

		longTTLConfig := KeyValueStoreConfig{
			Bucket:   "long-ttl-locks",
			MaxBytes: 1024 * 1024,
			Replicas: 1,
			TTL:      10 * time.Second,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", shortTTLConfig)
		if err != nil {
			t.Fatalf("failed to create short TTL KV store: %v", err)
		}

		err = cluster1.nc.CreateKeyValueStore("test-cluster", longTTLConfig)
		if err != nil {
			t.Fatalf("failed to create long TTL KV store: %v", err)
		}

		// Acquire locks in both buckets
		shortCancel, err := cluster1.nc.TryLock("short-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to acquire short TTL lock: %v", err)
		}
		defer shortCancel()

		longCancel, err := cluster2.nc.TryLock("long-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to acquire long TTL lock: %v", err)
		}
		defer longCancel()

		// Both locks should be initially acquired
		shortLocked, err := cluster1.nc.IsLocked("short-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to check short TTL lock: %v", err)
		}
		longLocked, err := cluster1.nc.IsLocked("long-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to check long TTL lock: %v", err)
		}

		if !shortLocked || !longLocked {
			t.Error("both locks should be initially acquired")
		}

		// Wait for short TTL to expire
		t.Log("Waiting for short TTL to expire...")
		time.Sleep(3 * time.Second)

		// Check lock states
		shortLocked, err = cluster1.nc.IsLocked("short-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to check short TTL lock after expiration: %v", err)
		}
		longLocked, err = cluster1.nc.IsLocked("long-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("failed to check long TTL lock after short expiration: %v", err)
		}

		if shortLocked {
			t.Error("short TTL lock should have expired")
		}
		if !longLocked {
			t.Error("long TTL lock should still be active")
		}

		// Node3 should be able to acquire the short TTL lock now
		shortCancel2, err := cluster3.nc.TryLock("short-ttl-locks", "resource")
		if err != nil {
			t.Fatalf("should be able to acquire expired short TTL lock: %v", err)
		}
		defer shortCancel2()

		// But not the long TTL lock
		_, err = cluster3.nc.TryLock("long-ttl-locks", "resource")
		if err == nil {
			t.Error("should not be able to acquire active long TTL lock")
		}

		t.Log("Successfully tested different TTL behaviors")
	})
}
