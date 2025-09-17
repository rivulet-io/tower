package tower

import (
	"fmt"

	"github.com/corvus-ch/shamir"
)

// SetShamirShare stores a set of Shamir secret shares
func (t *Tower) SetShamirShare(key string, shares map[byte][]byte) error {
	unlock := t.lock(key)
	defer unlock()

	df := NULLDataFrame()
	if err := df.SetShamirShare(shares); err != nil {
		return fmt.Errorf("failed to set Shamir share value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// GetShamirShare retrieves the Shamir secret shares
func (t *Tower) GetShamirShare(key string) (map[byte][]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return nil, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	return shares, nil
}

// SplitSecret splits a secret into n shares requiring threshold shares to reconstruct
func (t *Tower) SplitSecret(key string, secret []byte, n, threshold int) (map[byte][]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	shares, err := shamir.Split(secret, n, threshold)
	if err != nil {
		return nil, fmt.Errorf("failed to split secret: %w", err)
	}

	df := NULLDataFrame()
	if err := df.SetShamirShare(shares); err != nil {
		return nil, fmt.Errorf("failed to set Shamir share value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return nil, fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return shares, nil
}

// CombineShares reconstructs the secret from the stored shares
func (t *Tower) CombineShares(key string) ([]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return nil, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	secret, err := shamir.Combine(shares)
	if err != nil {
		return nil, fmt.Errorf("failed to combine shares: %w", err)
	}

	return secret, nil
}

// CombineSharesFrom reconstructs the secret from provided shares (not necessarily all stored shares)
func (t *Tower) CombineSharesFrom(shares map[byte][]byte) ([]byte, error) {
	if len(shares) == 0 {
		return nil, fmt.Errorf("shares cannot be nil or empty")
	}

	secret, err := shamir.Combine(shares)
	if err != nil {
		return nil, fmt.Errorf("failed to combine shares: %w", err)
	}

	return secret, nil
}

// GetShareCount returns the number of shares stored
func (t *Tower) GetShareCount(key string) (int, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return 0, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return 0, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	return len(shares), nil
}

// AddShare adds a single share to the existing shares
func (t *Tower) AddShare(key string, shareID byte, share []byte) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	// Add the new share
	shares[shareID] = make([]byte, len(share))
	copy(shares[shareID], share)

	if err := df.SetShamirShare(shares); err != nil {
		return fmt.Errorf("failed to set Shamir share value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// RemoveShare removes a specific share by ID
func (t *Tower) RemoveShare(key string, shareID byte) error {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	if _, exists := shares[shareID]; !exists {
		return fmt.Errorf("share with ID %d does not exist", shareID)
	}

	delete(shares, shareID)

	if err := df.SetShamirShare(shares); err != nil {
		return fmt.Errorf("failed to set Shamir share value: %w", err)
	}

	if err := t.set(key, df); err != nil {
		return fmt.Errorf("failed to set key %s: %w", key, err)
	}

	return nil
}

// HasShare checks if a specific share ID exists
func (t *Tower) HasShare(key string, shareID byte) (bool, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return false, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return false, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	_, exists := shares[shareID]
	return exists, nil
}

// GetShare retrieves a specific share by ID
func (t *Tower) GetShare(key string, shareID byte) ([]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return nil, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	share, exists := shares[shareID]
	if !exists {
		return nil, fmt.Errorf("share with ID %d does not exist", shareID)
	}

	result := make([]byte, len(share))
	copy(result, share)
	return result, nil
}

// ListShareIDs returns all share IDs
func (t *Tower) ListShareIDs(key string) ([]byte, error) {
	unlock := t.lock(key)
	defer unlock()

	df, err := t.get(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get key %s: %w", key, err)
	}

	shares, err := df.ShamirShare()
	if err != nil {
		return nil, fmt.Errorf("failed to get Shamir share value for key %s: %w", key, err)
	}

	shareIDs := make([]byte, 0, len(shares))
	for shareID := range shares {
		shareIDs = append(shareIDs, shareID)
	}

	return shareIDs, nil
}
