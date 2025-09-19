package mesh

import (
	"strings"
	"testing"
	"time"
)

func TestKeyValueStoreCreateBucket(t *testing.T) {
	t.Run("create bucket", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV store bucket
		config := KeyValueStoreConfig{
			Bucket:       "test-config",
			Description:  "Test configuration store",
			MaxValueSize: 1024,
			TTL:          24 * time.Hour,
			MaxBytes:     1024 * 1024, // 1MB
			Replicas:     3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		t.Log("Successfully created KV store bucket")
	})

	t.Run("create duplicate bucket", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		config := KeyValueStoreConfig{
			Bucket:   "duplicate-test",
			Replicas: 3,
		}

		// Create first bucket
		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create first KV store: %v", err)
		}

		// Try to create duplicate bucket (should handle gracefully)
		err = cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			// Expected error for duplicate bucket, check if it's the right error
			expectedError := "already exists"
			if !strings.Contains(err.Error(), expectedError) {
				t.Fatalf("unexpected error for duplicate bucket: %v", err)
			}
			t.Logf("Expected error for duplicate bucket: %v", err)
		} else {
			t.Log("Duplicate bucket creation succeeded (idempotent)")
		}

		t.Log("Successfully handled duplicate bucket creation")
	})
}

func TestKeyValueStorePutGet(t *testing.T) {
	t.Run("basic put and get", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create KV store
		config := KeyValueStoreConfig{
			Bucket:   "app-config",
			Replicas: 3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		// Wait for KV store to be ready
		time.Sleep(2 * time.Second)

		// Put values from different nodes
		testData := map[string][]byte{
			"database.host":  []byte("localhost:5432"),
			"database.user":  []byte("admin"),
			"api.rate_limit": []byte("1000"),
			"feature.new_ui": []byte("true"),
			"cache.ttl":      []byte("3600"),
		}

		for key, value := range testData {
			revision, err := cluster1.nc.PutToKeyValueStore("app-config", key, value)
			if err != nil {
				t.Fatalf("failed to put key %s: %v", key, err)
			}
			if revision == 0 {
				t.Errorf("expected non-zero revision for key %s", key)
			}
		}

		// Get values from different nodes to test replication
		clusters := []*Cluster{cluster1, cluster2, cluster3}
		for i, cluster := range clusters {
			for key, expectedValue := range testData {
				value, revision, err := cluster.nc.GetFromKeyValueStore("app-config", key)
				if err != nil {
					t.Fatalf("failed to get key %s from node %d: %v", key, i+1, err)
				}
				if string(value) != string(expectedValue) {
					t.Errorf("node %d: expected value %s for key %s, got %s", i+1, expectedValue, key, value)
				}
				if revision == 0 {
					t.Errorf("node %d: expected non-zero revision for key %s", i+1, key)
				}
			}
		}

		t.Logf("Successfully put and retrieved %d key-value pairs across all nodes", len(testData))
	})

	t.Run("non-existent key", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		config := KeyValueStoreConfig{
			Bucket:   "empty-store",
			Replicas: 3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		time.Sleep(1 * time.Second)

		// Try to get non-existent key
		_, _, err = cluster1.nc.GetFromKeyValueStore("empty-store", "non-existent")
		if err == nil {
			t.Error("expected error for non-existent key")
		}

		t.Log("Successfully handled non-existent key")
	})
}

func TestKeyValueStoreUpdate(t *testing.T) {
	t.Run("atomic update", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		config := KeyValueStoreConfig{
			Bucket:   "atomic-test",
			Replicas: 3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Put initial value
		key := "counter"
		initialValue := []byte("100")

		revision1, err := cluster1.nc.PutToKeyValueStore("atomic-test", key, initialValue)
		if err != nil {
			t.Fatalf("failed to put initial value: %v", err)
		}

		// Update with correct revision
		newValue := []byte("200")
		revision2, err := cluster2.nc.UpdateToKeyValueStore("atomic-test", key, newValue, revision1)
		if err != nil {
			t.Fatalf("failed to update with correct revision: %v", err)
		}

		if revision2 <= revision1 {
			t.Errorf("expected revision to increase: %d -> %d", revision1, revision2)
		}

		// Try to update with old revision (should fail)
		_, err = cluster3.nc.UpdateToKeyValueStore("atomic-test", key, []byte("300"), revision1)
		if err == nil {
			t.Error("expected error when updating with old revision")
		}

		// Verify final value
		value, revision, err := cluster1.nc.GetFromKeyValueStore("atomic-test", key)
		if err != nil {
			t.Fatalf("failed to get final value: %v", err)
		}

		if string(value) != string(newValue) {
			t.Errorf("expected final value %s, got %s", newValue, value)
		}

		if revision != revision2 {
			t.Errorf("expected final revision %d, got %d", revision2, revision)
		}

		t.Log("Successfully performed atomic update operations")
	})
}

func TestKeyValueStoreDelete(t *testing.T) {
	t.Run("delete key", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		config := KeyValueStoreConfig{
			Bucket:   "delete-test",
			Replicas: 3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		time.Sleep(2 * time.Second)

		// Put some values
		keys := []string{"temp1", "temp2", "temp3"}
		for i, key := range keys {
			value := []byte("temporary-value-" + string(rune('1'+i)))
			_, err := cluster1.nc.PutToKeyValueStore("delete-test", key, value)
			if err != nil {
				t.Fatalf("failed to put key %s: %v", key, err)
			}
		}

		// Delete one key
		err = cluster2.nc.DeleteFromKeyValueStore("delete-test", "temp2")
		if err != nil {
			t.Fatalf("failed to delete key: %v", err)
		}

		// Verify deletion across all nodes
		clusters := []*Cluster{cluster1, cluster2, cluster3}
		for i, cluster := range clusters {
			_, _, err := cluster.nc.GetFromKeyValueStore("delete-test", "temp2")
			if err == nil {
				t.Errorf("node %d: expected error for deleted key", i+1)
			}

			// Verify other keys still exist
			for _, key := range []string{"temp1", "temp3"} {
				_, _, err := cluster.nc.GetFromKeyValueStore("delete-test", key)
				if err != nil {
					t.Errorf("node %d: key %s should still exist: %v", i+1, key, err)
				}
			}
		}

		t.Log("Successfully deleted key and verified across all nodes")
	})
}

func TestKeyValueStoreTTL(t *testing.T) {
	t.Run("key expiration", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		config := KeyValueStoreConfig{
			Bucket:   "ttl-test",
			TTL:      2 * time.Second, // Short TTL for testing
			Replicas: 3,
		}

		err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create KV store: %v", err)
		}

		time.Sleep(1 * time.Second)

		// Put a value
		key := "expire-me"
		value := []byte("will-expire")

		_, err = cluster1.nc.PutToKeyValueStore("ttl-test", key, value)
		if err != nil {
			t.Fatalf("failed to put key: %v", err)
		}

		// Verify it exists immediately
		_, _, err = cluster2.nc.GetFromKeyValueStore("ttl-test", key)
		if err != nil {
			t.Fatalf("key should exist immediately: %v", err)
		}

		// Wait for TTL to expire
		time.Sleep(3 * time.Second)

		// Verify it's expired
		_, _, err = cluster3.nc.GetFromKeyValueStore("ttl-test", key)
		if err == nil {
			t.Error("expected key to be expired")
		}

		t.Log("Successfully verified TTL expiration")
	})
}

func TestKeyValueStoreMultipleBuckets(t *testing.T) {
	t.Run("multiple buckets", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create multiple buckets
		buckets := []KeyValueStoreConfig{
			{
				Bucket:      "user-sessions",
				Description: "User session data",
				Replicas:    3,
			},
			{
				Bucket:      "app-cache",
				Description: "Application cache",
				Replicas:    3,
			},
			{
				Bucket:      "feature-flags",
				Description: "Feature toggle flags",
				Replicas:    3,
			},
		}

		// Create all buckets
		for _, config := range buckets {
			err := cluster1.nc.CreateKeyValueStore("test-cluster", config)
			if err != nil {
				t.Fatalf("failed to create bucket %s: %v", config.Bucket, err)
			}
		}

		time.Sleep(2 * time.Second)

		// Put data into each bucket
		testData := map[string]map[string][]byte{
			"user-sessions": {
				"user123": []byte(`{"userId": "123", "loginTime": "2024-01-01T10:00:00Z"}`),
				"user456": []byte(`{"userId": "456", "loginTime": "2024-01-01T11:00:00Z"}`),
			},
			"app-cache": {
				"expensive-query-1": []byte(`{"result": [1,2,3,4,5], "cached": true}`),
				"expensive-query-2": []byte(`{"result": {"total": 100}, "cached": true}`),
			},
			"feature-flags": {
				"new-dashboard": []byte("true"),
				"beta-feature":  []byte("false"),
			},
		}

		// Insert data
		for bucket, data := range testData {
			for key, value := range data {
				_, err := cluster1.nc.PutToKeyValueStore(bucket, key, value)
				if err != nil {
					t.Fatalf("failed to put %s/%s: %v", bucket, key, err)
				}
			}
		}

		// Verify data isolation between buckets
		for bucket, data := range testData {
			for key, expectedValue := range data {
				// Get from correct bucket
				value, _, err := cluster2.nc.GetFromKeyValueStore(bucket, key)
				if err != nil {
					t.Fatalf("failed to get %s/%s: %v", bucket, key, err)
				}
				if string(value) != string(expectedValue) {
					t.Errorf("bucket %s: expected %s, got %s", bucket, expectedValue, value)
				}

				// Verify key doesn't exist in other buckets
				for otherBucket := range testData {
					if otherBucket != bucket {
						_, _, err := cluster3.nc.GetFromKeyValueStore(otherBucket, key)
						if err == nil {
							t.Errorf("key %s should not exist in bucket %s", key, otherBucket)
						}
					}
				}
			}
		}

		t.Logf("Successfully tested %d buckets with proper data isolation", len(buckets))
	})
}
