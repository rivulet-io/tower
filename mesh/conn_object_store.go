package mesh

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rivulet-io/tower/util/size"
)

type ObjectStoreConfig struct {
	Bucket      string            `json:"bucket"`
	Description string            `json:"description,omitempty"`
	TTL         time.Duration     `json:"max_age,omitempty"`
	MaxBytes    size.Size         `json:"max_bytes,omitempty"`
	Replicas    int               `json:"num_replicas,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (c *conn) CreateObjectStore(cluster string, config ObjectStoreConfig) error {
	storeConfig := nats.ObjectStoreConfig{
		Bucket:      config.Bucket,
		Description: config.Description,
		TTL:         config.TTL,
		MaxBytes:    config.MaxBytes.Bytes(),
		Replicas:    config.Replicas,
		Storage:     nats.FileStorage,
		Placement: &nats.Placement{
			Cluster: cluster,
		},
		Metadata:    config.Metadata,
		Compression: true,
	}
	_, err := c.js.CreateObjectStore(&storeConfig)
	if err != nil {
		if err == nats.ErrStreamNameAlreadyInUse {
			// Object store already exists, update is not supported
			return nil
		}

		return fmt.Errorf("failed to create or update object store: %w", err)
	}

	return nil
}

func (c *conn) GetFromObjectStore(bucket, key string) ([]byte, error) {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	obj, err := store.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %q from bucket %q: %w", key, bucket, err)
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to read object %q from bucket %q: %w", key, bucket, err)
	}

	return data, nil
}

func (c *conn) PutToObjectStore(bucket, key string, data []byte, metadata map[string]string) error {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	_, err = store.Put(&nats.ObjectMeta{
		Name:     key,
		Metadata: metadata,
	}, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to put object %q to bucket %q: %w", key, bucket, err)
	}

	return nil
}

func (c *conn) DeleteFromObjectStore(bucket, key string) error {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	err = store.Delete(key)
	if err != nil {
		return fmt.Errorf("failed to delete object %q from bucket %q: %w", key, bucket, err)
	}

	return nil
}

// Streaming support for large objects
func (c *conn) PutToObjectStoreStream(bucket, key string, reader io.Reader, metadata map[string]string) error {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	_, err = store.Put(&nats.ObjectMeta{
		Name:     key,
		Metadata: metadata,
	}, reader)
	if err != nil {
		return fmt.Errorf("failed to put object stream %q to bucket %q: %w", key, bucket, err)
	}

	return nil
}

func (c *conn) GetFromObjectStoreStream(bucket, key string) (io.ReadCloser, error) {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	obj, err := store.Get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %q from bucket %q: %w", key, bucket, err)
	}

	return obj, nil
}

// Object information and metadata
func (c *conn) GetObjectInfo(bucket, key string) (*nats.ObjectInfo, error) {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	info, err := store.GetInfo(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object info %q from bucket %q: %w", key, bucket, err)
	}

	return info, nil
}

func (c *conn) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	objects, err := store.List()
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in bucket %q: %w", bucket, err)
	}

	return objects, nil
}

func (c *conn) ObjectExists(bucket, key string) (bool, error) {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return false, fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	_, err = store.GetInfo(key)
	if err != nil {
		if err == nats.ErrObjectNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence %q in bucket %q: %w", key, bucket, err)
	}

	return true, nil
}

func (c *conn) DeleteObjectStore(bucket string) error {
	err := c.js.DeleteObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("failed to delete object store %q: %w", bucket, err)
	}

	return nil
}

// Chunked upload for very large files
func (c *conn) PutToObjectStoreChunked(bucket, key string, reader io.Reader, chunkSize int64, metadata map[string]string) error {
	store, err := c.js.ObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("failed to access object store %q: %w", bucket, err)
	}

	// Use a limited reader for chunking if chunkSize is specified
	var sourceReader io.Reader = reader
	if chunkSize > 0 {
		sourceReader = &io.LimitedReader{R: reader, N: chunkSize}
	}

	_, err = store.Put(&nats.ObjectMeta{
		Name:     key,
		Metadata: metadata,
	}, sourceReader)
	if err != nil {
		return fmt.Errorf("failed to put chunked object %q to bucket %q: %w", key, bucket, err)
	}

	return nil
}

// Copy object within or between buckets
func (c *conn) CopyObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error {
	// Get from source
	data, err := c.GetFromObjectStore(sourceBucket, sourceKey)
	if err != nil {
		return fmt.Errorf("failed to get source object: %w", err)
	}

	// Put to destination
	err = c.PutToObjectStore(destBucket, destKey, data, metadata)
	if err != nil {
		return fmt.Errorf("failed to put to destination: %w", err)
	}

	return nil
}

// Move object (copy + delete)
func (c *conn) MoveObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error {
	err := c.CopyObject(sourceBucket, sourceKey, destBucket, destKey, metadata)
	if err != nil {
		return fmt.Errorf("failed to copy object: %w", err)
	}

	err = c.DeleteFromObjectStore(sourceBucket, sourceKey)
	if err != nil {
		return fmt.Errorf("failed to delete source object: %w", err)
	}

	return nil
}
