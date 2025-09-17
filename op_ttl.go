package tower

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

const ttlBaseKey = "__system__:__ttl_list__"

func (t *Tower) makeTTLKey(timestamp int64) string {
	return ttlBaseKey + ":" + strconv.FormatInt(timestamp, 10)
}

const ttlPrecision = 1 * 60 * 1000 // 1 minutes in milliseconds

var currentTime = atomic.Pointer[time.Time]{}

func InitTimer() {
	now := time.Now()
	currentTime.Store(&now)
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			now := time.Now()
			currentTime.Store(&now)
		}
	}()
}

func Now() time.Time {
	t := currentTime.Load()
	if t == nil {
		return time.Now()
	}

	return *t
}

func (t *Tower) floorTTLTimestamp(criteria time.Time) int64 {
	v := criteria.UnixMilli()
	return v - (v % ttlPrecision)
}

func (t *Tower) ceilTTLTimestamp(criteria time.Time) int64 {
	v := criteria.UnixMilli()
	r := v % ttlPrecision
	if r == 0 {
		return v
	}
	return v + (ttlPrecision - r)
}

func (t *Tower) extractCandidatesForExpiration(criteria time.Time) ([]string, error) {
	v := t.floorTTLTimestamp(criteria)
	key := t.makeTTLKey(v)

	members, err := t.ListRange(key, 0, -1) // get all members
	if err != nil {
		return nil, fmt.Errorf("failed to get members from TTL list %s: %w", key, err)
	}

	result := make([]string, 0, len(members))
	for _, member := range members {
		str, err := member.String()
		if err == nil {
			result = append(result, str)
		}
	}

	return result, nil
}

func (t *Tower) addCandidatesForExpiration(key string, expireAt time.Time) error {
	v := t.ceilTTLTimestamp(expireAt)
	k := t.makeTTLKey(v)

	if _, err := t.PushRight(k, PrimitiveString(key)); err != nil {
		return fmt.Errorf("failed to add key %s to TTL list %s: %w", key, k, err)
	}

	return nil
}

func (t *Tower) SetTTL(key string, expireAt time.Time) error {
	now := Now()
	if !expireAt.After(now) {
		return nil // 이미 만료된 시간이면 무시
	}

	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	df.SetExpiration(expireAt)

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	if err := t.addCandidatesForExpiration(key, expireAt); err != nil {
		return fmt.Errorf("failed to add key %s to expiration candidates: %w", key, err)
	}

	return nil
}

func (t *Tower) RemoveTTL(key string) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	df.ClearExpiration()

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (t *Tower) TruncateExpired() error {
	now := Now()
	members, err := t.extractCandidatesForExpiration(now)
	if err != nil {
		return fmt.Errorf("failed to extract expiration candidates: %w", err)
	}

	for _, member := range members {
		func() {
			unlock := t.lock(member)
			defer unlock()
			df, err := t.get(member)
			if err == nil && !df.IsExpired(now) {
				if err := t.smartDelete(member, df.typ); err != nil {
					log.Printf("failed to delete expired key %s: %v", member, err)
				}
			}
		}()
	}

	return nil
}

func (t *Tower) StartTTLTimer() {
	go func() {
		ticker := time.NewTicker(ttlPrecision)
		for range ticker.C {
			if err := t.TruncateExpired(); err != nil {
				log.Printf("error truncating expired keys: %v", err)
			}
		}
	}()
}
