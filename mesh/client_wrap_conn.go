package mesh

import (
	"io"
	"time"

	"github.com/nats-io/nats.go"
)

// Ensure Client implements WrapConn interface
var _ WrapConn = (*Client)(nil)

// Note: Close() method is already implemented in client.go

func (c *Client) SetLogCallback(cb func(*NATSLog)) {
	if c.nc != nil {
		c.nc.SetLogCallback(cb)
	}
}

// Core messaging operations
func (c *Client) SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return c.nc.SubscribeVolatileViaFanout(subject, handler, errHandler)
}

func (c *Client) SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return c.nc.SubscribeVolatileViaQueue(subject, queue, handler, errHandler)
}

func (c *Client) PublishVolatile(subject string, msg []byte, headers ...nats.Header) error {
	return c.nc.PublishVolatile(subject, msg, headers...)
}

func (c *Client) RequestVolatile(subject string, msg []byte, timeout time.Duration, headers ...nats.Header) ([]byte, nats.Header, error) {
	return c.nc.RequestVolatile(subject, msg, timeout, headers...)
}

func (c *Client) PublishVolatileBatch(messages []struct {
	Subject string
	Data    []byte
	Headers nats.Header
}) error {
	return c.nc.PublishVolatileBatch(messages)
}

func (c *Client) FlushTimeout(timeout time.Duration) error {
	return c.nc.FlushTimeout(timeout)
}

// Stream operations
func (c *Client) CreateOrUpdateStream(cfg *PersistentConfig) error {
	return c.nc.CreateOrUpdateStream(cfg)
}

func (c *Client) SubscribeStreamViaDurable(subscriberID string, subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.SubscribeStreamViaDurable(subscriberID, subject, handler, errHandler, opt...)
}

func (c *Client) PullPersistentViaDurable(subscriberID string, subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.PullPersistentViaDurable(subscriberID, subject, option, handler, errHandler, opt...)
}

func (c *Client) SubscribePersistentViaEphemeral(subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.SubscribePersistentViaEphemeral(subject, handler, errHandler, opt...)
}

func (c *Client) PullPersistentViaEphemeral(subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.PullPersistentViaEphemeral(subject, option, handler, errHandler, opt...)
}

func (c *Client) PublishPersistent(subject string, msg []byte, opts ...nats.PubOpt) error {
	return c.nc.PublishPersistent(subject, msg, opts...)
}

func (c *Client) PublishPersistentWithOptions(subject string, msg []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return c.nc.PublishPersistentWithOptions(subject, msg, opts...)
}

func (c *Client) DeleteStream(streamName string) error {
	return c.nc.DeleteStream(streamName)
}

func (c *Client) GetStreamInfo(streamName string) (*nats.StreamInfo, error) {
	return c.nc.GetStreamInfo(streamName)
}

// KV Store operations
func (c *Client) CreateKeyValueStore(cluster string, config KeyValueStoreConfig) error {
	return c.nc.CreateKeyValueStore(cluster, config)
}

func (c *Client) GetFromKeyValueStore(bucket, key string) ([]byte, uint64, error) {
	return c.nc.GetFromKeyValueStore(bucket, key)
}

func (c *Client) PutToKeyValueStore(bucket, key string, value []byte) (uint64, error) {
	return c.nc.PutToKeyValueStore(bucket, key, value)
}

func (c *Client) UpdateToKeyValueStore(bucket, key string, value []byte, expectedRevision uint64) (uint64, error) {
	return c.nc.UpdateToKeyValueStore(bucket, key, value, expectedRevision)
}

func (c *Client) DeleteFromKeyValueStore(bucket, key string) error {
	return c.nc.DeleteFromKeyValueStore(bucket, key)
}

func (c *Client) PurgeKeyValueStore(bucket, key string) error {
	return c.nc.PurgeKeyValueStore(bucket, key)
}

func (c *Client) DeleteKeyValueStore(bucket string) error {
	return c.nc.DeleteKeyValueStore(bucket)
}

func (c *Client) KeyValueStoreExists(bucket string) bool {
	return c.nc.KeyValueStoreExists(bucket)
}

func (c *Client) ListKeysInKeyValueStore(bucket string) ([]string, error) {
	return c.nc.ListKeysInKeyValueStore(bucket)
}

func (c *Client) WatchKeyValueStore(bucket, key string) (nats.KeyWatcher, error) {
	return c.nc.WatchKeyValueStore(bucket, key)
}

func (c *Client) WatchAllKeysInKeyValueStore(bucket string) (nats.KeyWatcher, error) {
	return c.nc.WatchAllKeysInKeyValueStore(bucket)
}

// Object Store operations
func (c *Client) CreateObjectStore(cluster string, config ObjectStoreConfig) error {
	return c.nc.CreateObjectStore(cluster, config)
}

func (c *Client) GetFromObjectStore(bucket, key string) ([]byte, error) {
	return c.nc.GetFromObjectStore(bucket, key)
}

func (c *Client) PutToObjectStore(bucket, key string, data []byte, metadata map[string]string) error {
	return c.nc.PutToObjectStore(bucket, key, data, metadata)
}

func (c *Client) DeleteFromObjectStore(bucket, key string) error {
	return c.nc.DeleteFromObjectStore(bucket, key)
}

func (c *Client) PutToObjectStoreStream(bucket, key string, reader io.Reader, metadata map[string]string) error {
	return c.nc.PutToObjectStoreStream(bucket, key, reader, metadata)
}

func (c *Client) GetFromObjectStoreStream(bucket, key string) (io.ReadCloser, error) {
	return c.nc.GetFromObjectStoreStream(bucket, key)
}

func (c *Client) GetObjectInfo(bucket, key string) (*nats.ObjectInfo, error) {
	return c.nc.GetObjectInfo(bucket, key)
}

func (c *Client) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	return c.nc.ListObjects(bucket)
}

func (c *Client) ObjectExists(bucket, key string) (bool, error) {
	return c.nc.ObjectExists(bucket, key)
}

func (c *Client) DeleteObjectStore(bucket string) error {
	return c.nc.DeleteObjectStore(bucket)
}

func (c *Client) PutToObjectStoreChunked(bucket, key string, reader io.Reader, chunkSize int64, metadata map[string]string) error {
	return c.nc.PutToObjectStoreChunked(bucket, key, reader, chunkSize, metadata)
}

func (c *Client) CopyObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error {
	return c.nc.CopyObject(sourceBucket, sourceKey, destBucket, destKey, metadata)
}
