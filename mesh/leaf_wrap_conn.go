package mesh

import (
	"errors"
	"io"
	"time"

	"github.com/nats-io/nats.go"
)

// Ensure Leaf implements WrapConn interface
var _ WrapConn = (*Leaf)(nil)

var (
	ErrOperationNotPermittedForLeaf = errors.New("operation not permitted for leaf nodes")
)

// Note: Close() method is already implemented in leaf.go

func (l *Leaf) SetLogCallback(cb func(*NATSLog)) {
	if l.nc != nil {
		l.nc.SetLogCallback(cb)
	}
}

// Core messaging operations - All allowed for Leaf
func (l *Leaf) SubscribeVolatileViaFanout(subject string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return l.nc.SubscribeVolatileViaFanout(subject, handler, errHandler)
}

func (l *Leaf) SubscribeVolatileViaQueue(subject, queue string, handler func(subject string, msg []byte, headers nats.Header) ([]byte, nats.Header, bool), errHandler func(error)) (cancel func(), err error) {
	return l.nc.SubscribeVolatileViaQueue(subject, queue, handler, errHandler)
}

func (l *Leaf) PublishVolatile(subject string, msg []byte, headers ...nats.Header) error {
	return l.nc.PublishVolatile(subject, msg, headers...)
}

func (l *Leaf) RequestVolatile(subject string, msg []byte, timeout time.Duration, headers ...nats.Header) ([]byte, nats.Header, error) {
	return l.nc.RequestVolatile(subject, msg, timeout, headers...)
}

func (l *Leaf) PublishVolatileBatch(messages []struct {
	Subject string
	Data    []byte
	Headers nats.Header
}) error {
	return l.nc.PublishVolatileBatch(messages)
}

func (l *Leaf) FlushTimeout(timeout time.Duration) error {
	return l.nc.FlushTimeout(timeout)
}

// Stream operations - Read/Write allowed, Management not allowed
func (l *Leaf) CreateOrUpdateStream(cfg *PersistentConfig) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) SubscribeStreamViaDurable(subscriberID string, subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return l.nc.SubscribeStreamViaDurable(subscriberID, subject, handler, errHandler, opt...)
}

func (l *Leaf) PullPersistentViaDurable(subscriberID string, subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return l.nc.PullPersistentViaDurable(subscriberID, subject, option, handler, errHandler, opt...)
}

func (l *Leaf) SubscribePersistentViaEphemeral(subject string, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return l.nc.SubscribePersistentViaEphemeral(subject, handler, errHandler, opt...)
}

func (l *Leaf) PullPersistentViaEphemeral(subject string, option PullOptions, handler func(subject string, msg []byte) (response []byte, reply bool, ack bool), errHandler func(error), opt ...nats.SubOpt) (cancel func(), err error) {
	return l.nc.PullPersistentViaEphemeral(subject, option, handler, errHandler, opt...)
}

func (l *Leaf) PublishPersistent(subject string, msg []byte, opts ...nats.PubOpt) error {
	return l.nc.PublishPersistent(subject, msg, opts...)
}

func (l *Leaf) PublishPersistentWithOptions(subject string, msg []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return l.nc.PublishPersistentWithOptions(subject, msg, opts...)
}

func (l *Leaf) DeleteStream(streamName string) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) GetStreamInfo(streamName string) (*nats.StreamInfo, error) {
	return l.nc.GetStreamInfo(streamName)
}

// KV Store operations - Read/Write allowed, Store management not allowed
func (l *Leaf) CreateKeyValueStore(cluster string, config KeyValueStoreConfig) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) GetFromKeyValueStore(bucket, key string) ([]byte, uint64, error) {
	return l.nc.GetFromKeyValueStore(bucket, key)
}

func (l *Leaf) PutToKeyValueStore(bucket, key string, value []byte) (uint64, error) {
	return l.nc.PutToKeyValueStore(bucket, key, value)
}

func (l *Leaf) UpdateToKeyValueStore(bucket, key string, value []byte, expectedRevision uint64) (uint64, error) {
	return l.nc.UpdateToKeyValueStore(bucket, key, value, expectedRevision)
}

func (l *Leaf) DeleteFromKeyValueStore(bucket, key string) error {
	return l.nc.DeleteFromKeyValueStore(bucket, key)
}

func (l *Leaf) PurgeKeyValueStore(bucket, key string) error {
	return l.nc.PurgeKeyValueStore(bucket, key)
}

func (l *Leaf) DeleteKeyValueStore(bucket string) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) KeyValueStoreExists(bucket string) bool {
	return l.nc.KeyValueStoreExists(bucket)
}

func (l *Leaf) ListKeysInKeyValueStore(bucket string) ([]string, error) {
	return l.nc.ListKeysInKeyValueStore(bucket)
}

func (l *Leaf) WatchKeyValueStore(bucket, key string) (nats.KeyWatcher, error) {
	return l.nc.WatchKeyValueStore(bucket, key)
}

func (l *Leaf) WatchAllKeysInKeyValueStore(bucket string) (nats.KeyWatcher, error) {
	return l.nc.WatchAllKeysInKeyValueStore(bucket)
}

// Object Store operations - Read/Write allowed, Store management not allowed
func (l *Leaf) CreateObjectStore(cluster string, config ObjectStoreConfig) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) GetFromObjectStore(bucket, key string) ([]byte, error) {
	return l.nc.GetFromObjectStore(bucket, key)
}

func (l *Leaf) PutToObjectStore(bucket, key string, data []byte, metadata map[string]string) error {
	return l.nc.PutToObjectStore(bucket, key, data, metadata)
}

func (l *Leaf) DeleteFromObjectStore(bucket, key string) error {
	return l.nc.DeleteFromObjectStore(bucket, key)
}

func (l *Leaf) PutToObjectStoreStream(bucket, key string, reader io.Reader, metadata map[string]string) error {
	return l.nc.PutToObjectStoreStream(bucket, key, reader, metadata)
}

func (l *Leaf) GetFromObjectStoreStream(bucket, key string) (io.ReadCloser, error) {
	return l.nc.GetFromObjectStoreStream(bucket, key)
}

func (l *Leaf) GetObjectInfo(bucket, key string) (*nats.ObjectInfo, error) {
	return l.nc.GetObjectInfo(bucket, key)
}

func (l *Leaf) ListObjects(bucket string) ([]*nats.ObjectInfo, error) {
	return l.nc.ListObjects(bucket)
}

func (l *Leaf) ObjectExists(bucket, key string) (bool, error) {
	return l.nc.ObjectExists(bucket, key)
}

func (l *Leaf) DeleteObjectStore(bucket string) error {
	return ErrOperationNotPermittedForLeaf
}

func (l *Leaf) PutToObjectStoreChunked(bucket, key string, reader io.Reader, chunkSize int64, metadata map[string]string) error {
	return l.nc.PutToObjectStoreChunked(bucket, key, reader, chunkSize, metadata)
}

func (l *Leaf) CopyObject(sourceBucket, sourceKey, destBucket, destKey string, metadata map[string]string) error {
	return l.nc.CopyObject(sourceBucket, sourceKey, destBucket, destKey, metadata)
}

// Advisory operations
func (l *Leaf) SubscribeLeaderChange(stream string, handler func(stream string, leader string, myName string), errHandler func(error)) (cancel func(), err error) {
	return l.nc.SubscribeLeaderChange(stream, handler, errHandler)
}
