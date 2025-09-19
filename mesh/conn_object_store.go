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
