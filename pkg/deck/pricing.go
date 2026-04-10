package deck

import "github.com/papercomputeco/tapes/pkg/sessions"

// PricingTable aliases sessions.PricingTable so the deck and the API speak
// the same map type without conversion at every boundary.
type PricingTable = sessions.PricingTable

// DefaultPricing returns the canonical pricing table from pkg/sessions.
// Re-exported as a function so consumers that already imported
// deck.DefaultPricing keep working without an import change.
func DefaultPricing() PricingTable {
	return sessions.DefaultPricing()
}
