package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rivulet-io/tower"
)

func main() {
	// Create an in-memory Tower instance
	t, err := tower.NewTower(&tower.Options{
		Path:         "data",
		FS:           tower.InMemory(),
		CacheSize:    tower.NewSizeFromMegabytes(64),
		MemTableSize: tower.NewSizeFromMegabytes(16),
		BytesPerSync: tower.NewSizeFromKilobytes(512),
	})
	if err != nil {
		log.Fatalf("Failed to create tower: %v", err)
	}
	defer t.Close()

	// Initialize the timer for Now() function
	tower.InitTimer()

	// Set some keys with TTL
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	ttls := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second}

	for i, key := range keys {
		err := t.SetString(key, fmt.Sprintf("value%d", i+1))
		if err != nil {
			log.Printf("Failed to set key %s: %v", key, err)
			continue
		}

		expireAt := time.Now().Add(ttls[i])
		err = t.SetTTL(key, expireAt)
		if err != nil {
			log.Printf("Failed to set TTL for key %s: %v", key, err)
			continue
		}

		log.Printf("Set key %s with TTL %v (expires at %v)", key, ttls[i], expireAt)
	}

	// Start the TTL timer for periodic cleanup
	t.StartTTLTimer()
	log.Println("Started TTL timer for periodic cleanup")

	// Track key existence
	keyExists := make(map[string]bool)
	for _, key := range keys {
		keyExists[key] = true
	}

	// Monitor keys periodically
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			allDeleted := true
			for _, key := range keys {
				if !keyExists[key] {
					continue // Already logged as deleted
				}

				_, err := t.GetString(key)
				if err != nil {
					// Key is deleted or expired
					log.Printf("Key %s deleted at %v", key, time.Now().Format("2006-01-02 15:04:05"))
					keyExists[key] = false
				} else {
					allDeleted = false
				}
			}

			if allDeleted {
				log.Printf("All keys expired and deleted at %v. Application exiting.", time.Now().Format("2006-01-02 15:04:05"))
				os.Exit(0)
			}
		}
	}
}
