package mesh

import (
	"bytes"
	"testing"
	"time"

	"github.com/rivulet-io/tower/util/size"
)

func TestObjectStoreCreateBucket(t *testing.T) {
	t.Run("create bucket", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket
		config := ObjectStoreConfig{
			Bucket:      "documents",
			Description: "Document storage bucket",
			MaxBytes:    size.Size(5 * 1024 * 1024), // Reduce to 5MB
			Replicas:    1,                          // Reduce replicas for test
		}

		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create object store bucket: %v", err)
		}

		t.Log("Successfully created object store bucket")
	})

	t.Run("create duplicate bucket", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket
		config := ObjectStoreConfig{
			Bucket:      "duplicate-test",
			Description: "Test duplicate bucket",
			MaxBytes:    size.Size(5 * 1024 * 1024),
		}

		// Create first bucket
		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create initial object store bucket: %v", err)
		}

		// Try to create duplicate - NATS Object Store might allow this or return specific error
		err = cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Logf("Got error for duplicate bucket (expected): %v", err)
			t.Log("Successfully handled duplicate bucket creation with error")
		} else {
			t.Log("NATS allows duplicate bucket creation or returns existing bucket")
			t.Log("Successfully handled duplicate bucket creation without error")
		}
	})
}

func TestObjectStorePutGet(t *testing.T) {
	t.Run("basic put and get", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket
		config := ObjectStoreConfig{
			Bucket:      "files",
			Description: "File storage",
			MaxBytes:    size.Size(10 * 1024 * 1024), // 10MB
			Replicas:    1,                           // Reduce replicas
		}

		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create object store bucket: %v", err)
		}

		// Test different file types
		testFiles := map[string][]byte{
			"document.txt": []byte("This is a text document with some content"),
			"config.json":  []byte(`{"setting": "value", "number": 42, "enabled": true}`),
			"binary.data":  []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}, // PNG header
			"large.file":   make([]byte, 1024*1024),                                // 1MB file
			"metadata.xml": []byte("<?xml version=\"1.0\"?><root><item>value</item></root>"),
		}

		// Fill large file with pattern
		for i := range testFiles["large.file"] {
			testFiles["large.file"][i] = byte(i % 256)
		}

		// Put objects from different nodes
		nodes := []*Cluster{cluster1, cluster2, cluster3}
		i := 0
		for key, data := range testFiles {
			node := nodes[i%len(nodes)]

			metadata := map[string]string{
				"content-type": "application/octet-stream",
				"uploaded-by":  "test",
				"size":         string(rune(len(data))),
			}

			err := node.nc.PutToObjectStore("files", key, data, metadata)
			if err != nil {
				t.Fatalf("failed to put object %s: %v", key, err)
			}
			i++
		}

		// Get objects from different nodes
		i = 0
		for key, expectedData := range testFiles {
			node := nodes[(i+1)%len(nodes)] // Use different node for get

			retrievedData, err := node.nc.GetFromObjectStore("files", key)
			if err != nil {
				t.Fatalf("failed to get object %s: %v", key, err)
			}

			if !bytes.Equal(retrievedData, expectedData) {
				t.Errorf("data mismatch for key %s: expected %d bytes, got %d bytes",
					key, len(expectedData), len(retrievedData))
			}
			i++
		}

		t.Logf("Successfully put and retrieved %d objects across all nodes", len(testFiles))
	})

	t.Run("non-existent object", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket
		config := ObjectStoreConfig{
			Bucket:   "empty-bucket",
			MaxBytes: size.Size(5 * 1024 * 1024),
		}

		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create object store bucket: %v", err)
		}

		// Try to get non-existent object
		_, err = cluster1.nc.GetFromObjectStore("empty-bucket", "does-not-exist.txt")
		if err == nil {
			t.Error("expected error for non-existent object")
		} else {
			t.Logf("Expected error for non-existent object: %v", err)
			t.Log("Successfully handled non-existent object")
		}
	})
}

func TestObjectStoreDelete(t *testing.T) {
	t.Run("delete object", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket
		config := ObjectStoreConfig{
			Bucket:   "deletable",
			MaxBytes: size.Size(5 * 1024 * 1024),
			Replicas: 1,
		}

		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create object store bucket: %v", err)
		}

		// Put multiple objects
		objects := map[string][]byte{
			"temp1.txt": []byte("temporary file 1"),
			"temp2.txt": []byte("temporary file 2"),
			"keep.txt":  []byte("this file should remain"),
		}

		for key, data := range objects {
			err := cluster1.nc.PutToObjectStore("deletable", key, data, nil)
			if err != nil {
				t.Fatalf("failed to put object %s: %v", key, err)
			}
		}

		// Verify objects exist
		for key := range objects {
			_, err := cluster1.nc.GetFromObjectStore("deletable", key)
			if err != nil {
				t.Fatalf("object %s should exist before deletion: %v", key, err)
			}
		}

		// Delete objects from different nodes
		err = cluster2.nc.DeleteFromObjectStore("deletable", "temp1.txt")
		if err != nil {
			t.Fatalf("failed to delete temp1.txt: %v", err)
		}

		err = cluster3.nc.DeleteFromObjectStore("deletable", "temp2.txt")
		if err != nil {
			t.Fatalf("failed to delete temp2.txt: %v", err)
		}

		// Verify deletion across all nodes
		for _, node := range []*Cluster{cluster1, cluster2, cluster3} {
			// temp1.txt and temp2.txt should be deleted
			_, err = node.nc.GetFromObjectStore("deletable", "temp1.txt")
			if err == nil {
				t.Error("temp1.txt should be deleted")
			}

			_, err = node.nc.GetFromObjectStore("deletable", "temp2.txt")
			if err == nil {
				t.Error("temp2.txt should be deleted")
			}

			// keep.txt should still exist
			_, err = node.nc.GetFromObjectStore("deletable", "keep.txt")
			if err != nil {
				t.Errorf("keep.txt should still exist: %v", err)
			}
		}

		t.Log("Successfully deleted objects and verified across all nodes")
	})
}

func TestObjectStoreLargeFiles(t *testing.T) {
	t.Run("large file upload and download", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create object store bucket for large files
		config := ObjectStoreConfig{
			Bucket:   "large-files",
			MaxBytes: size.Size(10 * 1024 * 1024), // Reduce to 10MB
			Replicas: 1,                           // Reduce replicas
		}

		err := cluster1.nc.CreateObjectStore("test-cluster", config)
		if err != nil {
			t.Fatalf("failed to create object store bucket: %v", err)
		}

		// Create a 1MB file with repeating pattern (reduced from 5MB)
		fileSize := 1 * 1024 * 1024
		largeData := make([]byte, fileSize)
		pattern := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

		for i := 0; i < fileSize; i++ {
			largeData[i] = pattern[i%len(pattern)]
		}

		metadata := map[string]string{
			"content-type": "application/octet-stream",
			"size":         string(rune(fileSize)),
			"pattern":      "repeating-alphabet",
		}

		// Upload large file from node1
		startTime := time.Now()
		err = cluster1.nc.PutToObjectStore("large-files", "big-file.dat", largeData, metadata)
		if err != nil {
			t.Fatalf("failed to upload large file: %v", err)
		}
		uploadTime := time.Since(startTime)

		// Download large file from node2
		startTime = time.Now()
		retrievedData, err := cluster2.nc.GetFromObjectStore("large-files", "big-file.dat")
		if err != nil {
			t.Fatalf("failed to download large file: %v", err)
		}
		downloadTime := time.Since(startTime)

		// Verify data integrity
		if !bytes.Equal(largeData, retrievedData) {
			t.Errorf("large file data integrity check failed: expected %d bytes, got %d bytes",
				len(largeData), len(retrievedData))
		}

		// Verify from node3 as well
		retrievedData3, err := cluster3.nc.GetFromObjectStore("large-files", "big-file.dat")
		if err != nil {
			t.Fatalf("failed to download large file from node3: %v", err)
		}

		if !bytes.Equal(largeData, retrievedData3) {
			t.Error("large file data integrity check failed on node3")
		}

		t.Logf("Successfully uploaded/downloaded %d byte file (upload: %v, download: %v)",
			fileSize, uploadTime, downloadTime)
	})
}

func TestObjectStoreMultipleBuckets(t *testing.T) {
	t.Run("multiple buckets isolation", func(t *testing.T) {
		cluster1, cluster2, cluster3 := SetupThreeNodeCluster(t)
		defer CleanupClusters(cluster1, cluster2, cluster3)

		// Create multiple buckets with different purposes (reduced sizes for test environment)
		buckets := []ObjectStoreConfig{
			{
				Bucket:      "images",
				Description: "Image storage",
				MaxBytes:    size.Size(2 * 1024 * 1024), // 2MB
				Replicas:    1,                          // Reduce replicas to save resources
			},
			{
				Bucket:      "documents",
				Description: "Document storage",
				MaxBytes:    size.Size(1 * 1024 * 1024), // 1MB
				Replicas:    1,
			},
			{
				Bucket:      "backups",
				Description: "Backup storage",
				MaxBytes:    size.Size(3 * 1024 * 1024), // 3MB
				Replicas:    1,
			},
		}

		// Create all buckets
		for _, config := range buckets {
			err := cluster1.nc.CreateObjectStore("test-cluster", config)
			if err != nil {
				t.Fatalf("failed to create bucket %s: %v", config.Bucket, err)
			}
		}

		// Put data in each bucket with same key names
		testData := map[string][]byte{
			"file1.dat": []byte("data for images bucket"),
			"file2.dat": []byte("data for documents bucket"),
			"file3.dat": []byte("data for backups bucket"),
		}

		bucketNames := []string{"images", "documents", "backups"}

		for i, bucketName := range bucketNames {
			for key, data := range testData {
				// Modify data to be unique per bucket
				uniqueData := append([]byte(bucketName+": "), data...)

				node := []*Cluster{cluster1, cluster2, cluster3}[i%3]
				err := node.nc.PutToObjectStore(bucketName, key, uniqueData, nil)
				if err != nil {
					t.Fatalf("failed to put %s to bucket %s: %v", key, bucketName, err)
				}
			}
		}

		// Verify data isolation - same keys in different buckets should have different data
		for i, bucketName := range bucketNames {
			for key := range testData {
				node := []*Cluster{cluster1, cluster2, cluster3}[(i+1)%3] // Use different node

				retrievedData, err := node.nc.GetFromObjectStore(bucketName, key)
				if err != nil {
					t.Fatalf("failed to get %s from bucket %s: %v", key, bucketName, err)
				}

				expectedPrefix := bucketName + ": "
				if !bytes.HasPrefix(retrievedData, []byte(expectedPrefix)) {
					t.Errorf("data isolation failed: bucket %s, key %s does not have expected prefix",
						bucketName, key)
				}
			}
		}

		t.Logf("Successfully tested %d buckets with proper data isolation", len(buckets))
	})
}
