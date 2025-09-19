package op

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const ttlBaseKey = "__system__:__ttl_list__"

func (op *Operator) makeTTLKey(timestamp int64) string {
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

func (op *Operator) floorTTLTimestamp(criteria time.Time) int64 {
	v := criteria.UnixMilli()
	return v - (v % ttlPrecision)
}

func (op *Operator) ceilTTLTimestamp(criteria time.Time) int64 {
	v := criteria.UnixMilli()
	r := v % ttlPrecision
	if r == 0 {
		return v
	}
	return v + (ttlPrecision - r)
}

func (op *Operator) extractCandidatesForExpiration(criteria time.Time) ([]string, error) {
	v := op.floorTTLTimestamp(criteria)
	key := op.makeTTLKey(v)

	members, err := op.ListGetAllMembersAndDelete(key)
	if err != nil {
		// If the list does not exist, return empty list
		return []string{}, nil
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

func (op *Operator) addCandidatesForExpiration(key string, expireAt time.Time) error {
	v := op.ceilTTLTimestamp(expireAt)
	k := op.makeTTLKey(v)

	// Ensure the TTL list exists
	if err := op.CreateList(k); err != nil && !strings.Contains(err.Error(), "already exists") {
		return fmt.Errorf("failed to create TTL list %s: %w", k, err)
	}

	if _, err := op.PushRight(k, PrimitiveString(key)); err != nil {
		return fmt.Errorf("failed to add key %s to TTL list %s: %w", key, k, err)
	}

	return nil
}

func (op *Operator) SetTTL(key string, expireAt time.Time) error {
	now := Now()
	if !expireAt.After(now) {
		return nil // 이미 만료된 시간이면 무시
	}

	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	df.SetExpiration(expireAt)

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	if err := op.addCandidatesForExpiration(key, expireAt); err != nil {
		return fmt.Errorf("failed to add key %s to expiration candidates: %w", key, err)
	}

	return nil
}

func (op *Operator) RemoveTTL(key string) error {
	unlock := op.lock(key)
	defer unlock()

	df, err := op.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	df.ClearExpiration()

	if err := op.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

func (op *Operator) TruncateExpired() error {
	now := Now()
	members, err := op.extractCandidatesForExpiration(now)
	if err != nil {
		return fmt.Errorf("failed to extract expiration candidates: %w", err)
	}

	for _, member := range members {
		func() {
			unlock := op.lock(member)
			defer unlock()
			df, err := op.get(member)
			if err == nil && !df.IsExpired(now) {
				if err := op.smartDelete(member, df.typ); err != nil {
					log.Printf("failed to delete expired key %s: %v", member, err)
				}
			}
		}()
	}

	return nil
}

func (op *Operator) StartTTLTimer() {
	go func() {
		ticker := time.NewTicker(ttlPrecision)
		for range ticker.C {
			if err := op.TruncateExpired(); err != nil {
				log.Printf("error truncating expired keys: %v", err)
			}
		}
	}()
}
