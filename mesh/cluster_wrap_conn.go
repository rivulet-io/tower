package mesh

import (
	"io"
	"time"

	"github.com/nats-io/nats.go"
)

// Ensure Cluster implements WrapConn interface
var _ WrapConn = (*Cluster)(nil)

// Note: Close() method is already implemented in cluster.go

func (c *Cluster) SetLogCallback(cb func(*NATSLog)) {
	if c.nc != nil {
		c.nc.SetLogCallback(cb)
	}
}

// Core messaging operations
func (c *Cluster) SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return c.nc.SubscribeVolatileViaFanout(subject, handler, errHandler)
}

func (c *Cluster) SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return c.nc.SubscribeVolatileViaQueue(subject, queue, handler, errHandler)
}

func (c *Cluster) PublishVolatile(subject string, msg []byte, headers ...nats.Header) error {
	return c.nc.PublishVolatile(subject, msg, headers...)
}

func (c *Cluster) RequestVolatile(subject string, msg []byte, timeout time.Duration, headers ...nats.Header) ([]byte, nats.Header, error) {
	return c.nc.RequestVolatile(subject, msg, timeout, headers...)
}

func (c *Cluster) PublishVolatileBatch(messages []struct {
	Subject string
	Data    []byte
	Headers nats.Header
}) error {
	return c.nc.PublishVolatileBatch(messages)
}

func (c *Cluster) FlushTimeout(timeout time.Duration) error {
	return c.nc.FlushTimeout(timeout)
}

// Stream operations
func (c *Cluster) CreateOrUpdateStream(cfg *PersistentConfig) error {
	return c.nc.CreateOrUpdateStream(cfg)
}

func (c *Cluster) SubscribeStreamViaDurable(subscriberID string, subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.SubscribeStreamViaDurable(subscriberID, subject, handler, errHandler, opt...)
}

func (c *Cluster) PullPersistentViaDurable(subscriberID string, subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.PullPersistentViaDurable(subscriberID, subject, option, handler, errHandler, opt...)
}

func (c *Cluster) SubscribePersistentViaEphemeral(subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.SubscribePersistentViaEphemeral(subject, handler, errHandler, opt...)
}

func (c *Cluster) PullPersistentViaEphemeral(subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return c.nc.PullPersistentViaEphemeral(subject, option, handler, errHandler, opt...)
}

func (c *Cluster) PublishPersistent(subject string, msg []byte, opts ...nats.PubOpt) error {
	return c.nc.PublishPersistent(subject, msg, opts...)
}

func (c *Cluster) PublishPersistentWithOptions(subject string, msg []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return c.nc.PublishPersistentWithOptions(subject, msg, opts...)
}

func (c *Cluster) DeleteStream(streamName string) error {
	return c.nc.DeleteStream(streamName)
}

func (c *Cluster) GetStreamInfo(streamName string) (*nats.StreamInfo, error) {
	return c.nc.GetStreamInfo(streamName)
}

// KV Store operations
func (c *Cluster) CreateKeyValueStore(cluster string, config KeyValueStoreConfig) error {
	return c.nc.CreateKeyValueStore(cluster, config)
}

func (c *Cluster) GetFromKeyValueStore(bucket, key string) ([]byte, uint64, error) {
	return c.nc.GetFromKeyValueStore(bucket, key)
}

func (c *Cluster) PutToKeyValueStore(bucket, key string, value []byte) (uint64, error) {
	return c.nc.PutToKeyValueStore(bucket, key, value)
}

func (c *Cluster) UpdateToKeyValueStore(bucket, key string, value []byte, expectedRevision uint64) (uint64, error) {
	return c.nc.UpdateToKeyValueStore(bucket, key, value, expectedRevision)
}

func (c *Cluster) DeleteFromKeyValueStore(bucket, key string) error {
	return c.nc.DeleteFromKeyValueStore(bucket, key)
}

func (c *Cluster) PurgeKeyValueStore(bucket, key string) error {
	return c.nc.PurgeKeyValueStore(bucket, key)
}

func (c *Cluster) DeleteKeyValueStore(bucket string) error {
	return c.nc.DeleteKeyValueStore(bucket)
}

func (c *Cluster) KeyValueStoreExists(bucket string) bool {
	return c.nc.KeyValueStoreExists(bucket)
}

func (c *Cluster) ListKeysInKeyValueStore(bucket string) ([]string, error) {
	return c.nc.ListKeysInKeyValueStore(bucket)
}

func (c *Cluster) WatchKeyValueStore(bucket, key string) (nats.KeyWatcher, error) {
	return c.nc.WatchKeyValueStore(bucket, key)
}

func (c *Cluster) WatchAllKeysInKeyValueStore(bucket string) (nats.KeyWatcher, error) {
	return c.nc.WatchAllKeysInKeyValueStore(bucket)
}

// Object Store operations
func (c *Cluster) CreateObjectStore(cluster string, config ObjectStoreConfig) error {
	return c.nc.CreateObjectStore(cluster, config)
}

func (c *Cluster) GetFromObjectStore(bucket, key string) ([]byte, error) {
	return c.nc.GetFromObjectStore(bucket, key)
}

func (c *Cluster) PutToObjectStore(bucket, key string, data []byte, metadata map[string]string) error {
	return c.nc.PutToObjectStore(bucket, key, data, metadata)
}

func (c *Cluster) DeleteFromObjectStore(bucket, key string) error {
	return c.nc.DeleteFromObjectStore(bucket, key)
}

func (c *Cluster) PutToObjectStoreStream(bucket, key string, reader io.Reader, metadata map[string]string) error {
	return c.nc.PutToObjectStoreStream(bucket, key, reader, metadata)
}

func (c *Cluster) GetFromObjectStoreStream(bucket, key string) (io.ReadCloser, error) {
	return c.nc.GetFromObjectStoreStream(bucket, key)
}

func (c *Cluster) GetObjectInfo(bucket, key string) (*nats.ObjectInfo, error) {
	return c.nc.GetObjectInfo(bucket, key)
}

func (c *Cluster) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	return c.nc.ListObjects(bucket)
}

func (c *Cluster) ObjectExists(bucket, key string) (bool, error) {
	return c.nc.ObjectExists(bucket, key)
}

func (c *Cluster) DeleteObjectStore(bucket string) error {
	return c.nc.DeleteObjectStore(bucket)
}

func (c *Cluster) PutToObjectStoreChunked(bucket, key string, reader io.Reader, chunkSize int64, metadata map[string]string) error {
	return c.nc.PutToObjectStoreChunked(bucket, key, reader, chunkSize, metadata)
}

func (c *Cluster) CopyObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error {
	return c.nc.CopyObject(sourceBucket, sourceKey, destBucket, destKey, metadata)
}

// Advisory operations
func (c *Cluster) SubscribeLeaderChange(stream string, handler func(stream string, leader string, myName string), errHandler func(error)) (cancel func(), err error) {
	return c.nc.SubscribeLeaderChange(stream, handler, errHandler)
}
