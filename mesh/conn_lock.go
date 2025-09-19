package mesh

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

const lockValue = "__locked__"

func (c *conn) TryLock(bucket, key string) (cancel func(), err error) {
	kv, err := c.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to access key-value store %q: %w", bucket, err)
	}

	revision, err := kv.Create(key, []byte(lockValue))
	if err != nil {
		return nil, fmt.Errorf("failed to lock key %q in bucket %q: %w", key, bucket, err)
	}

	return func() {
		_ = kv.Delete(key, nats.LastRevision(revision))
	}, nil
}

type LockOptions struct {
	initialDelay  time.Duration
	MaxDelay      time.Duration
	BackOffFactor int
}

func (c *conn) Lock(ctx context.Context, bucket, key string, opt ...LockOptions) (cancel func(), err error) {
	option := LockOptions{
		initialDelay:  time.Millisecond * 10,
		MaxDelay:      2 * time.Second,
		BackOffFactor: 2,
	}
	if len(opt) > 0 {
		option = opt[0]
	}

	currentDelay := option.initialDelay
	backOffFactor := time.Duration(option.BackOffFactor)
	maxDelay := option.MaxDelay

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		cancel, err = c.TryLock(bucket, key)
		if err == nil {
			return cancel, nil
		}
		if !errors.Is(err, nats.ErrKeyExists) {
			return nil, fmt.Errorf("failed to lock key %q in bucket %q: %w", key, bucket, err)
		}
		time.Sleep(currentDelay)
		currentDelay *= backOffFactor
		if currentDelay > maxDelay {
			currentDelay = maxDelay
		}
	}
}

func (c *conn) ForceUnlock(bucket, key string) error {
	kv, err := c.js.KeyValue(bucket)
	if err != nil {
		return fmt.Errorf("failed to access key-value store %q: %w", bucket, err)
	}

	if err := kv.Delete(key); err != nil {
		return fmt.Errorf("failed to force unlock key %q in bucket %q: %w", key, bucket, err)
	}

	return nil
}

func (c *conn) IsLocked(bucket, key string) (bool, error) {
	kv, err := c.js.KeyValue(bucket)
	if err != nil {
		return false, fmt.Errorf("failed to access key-value store %q: %w", bucket, err)
	}

	_, err = kv.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return false, nil
		}
		return false, fmt.Errorf("failed to get key %q from bucket %q: %w", key, bucket, err)
	}

	return true, nil
}
