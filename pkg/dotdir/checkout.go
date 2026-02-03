package dotdir

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	checkoutFile = "checkout.json"
)

// CheckoutState represents the persisted checkout state.
// It contains the hash of the checked-out node and the conversation messages
// leading up to (and including) that node.
type CheckoutState struct {
	// Hash is the hash of the checked-out node.
	Hash string `json:"hash"`

	// Messages is the conversation history in chronological order
	// (oldest first), up to and including the checked-out node.
	Messages []CheckoutMessage `json:"messages"`
}

// CheckoutMessage represents a single message in the checked-out conversation.
type CheckoutMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// LoadCheckoutState loads the checkout state from a target .tapes/checkout.json.
// Returns nil, nil if no checkout state exists (empty/new conversation state).
// If overrideDir is non-empty, it is used instead of the default ~/.tapes/ location.
func (m *Manager) LoadCheckoutState(overrideDir string) (*CheckoutState, error) {
	dir, err := m.Target(overrideDir)
	if err != nil {
		return nil, err
	}

	path := filepath.Join(dir, checkoutFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading checkout state: %w", err)
	}

	state := &CheckoutState{}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing checkout state: %w", err)
	}

	return state, nil
}

// SaveCheckout persists the checkout state to a target .tapes/checkout.json.
func (m *Manager) SaveCheckout(state *CheckoutState, overrideDir string) error {
	if state == nil {
		return errors.New("cannot save nil checkout state")
	}

	dir, err := m.Target(overrideDir)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling checkout state: %w", err)
	}

	path := filepath.Join(dir, checkoutFile)
	if err := os.WriteFile(path, data, 0o644); err != nil { //nolint:gosec // @jpmcb - TODO: refactor file permissions
		return fmt.Errorf("writing checkout state: %w", err)
	}

	return nil
}

// ClearCheckout removes the checkout state file.
// This resets the state so the next chat session starts a new root conversation.
// If overrideDir is non-empty, it is used instead of the default ~/.tapes/ location.
// Returns nil if the file doesn't exist (already cleared).
func (m *Manager) ClearCheckout(overrideDir string) error {
	dir, err := m.Target(overrideDir)
	if err != nil {
		return err
	}

	path := filepath.Join(dir, checkoutFile)
	if err := os.Remove(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("removing checkout state: %w", err)
	}

	return nil
}
