package postgres

import "github.com/papercomputeco/tapes/pkg/merkle"

func ToMigrateDSNForTest(dsn string) string {
	return toMigrateDSN(dsn)
}

func InterfaceInt32ForTest(v any) int32 {
	return interfaceInt32(v)
}

func InterfaceInt64ForTest(v any) int64 {
	return interfaceInt64(v)
}

// ValidateChainOrderingForTest re-exports the package-internal chain
// ordering validator so the unit suite can exercise it without an
// integration database.
func ValidateChainOrderingForTest(nodes []*merkle.Node) error {
	_, err := validateChainOrdering(nodes)
	return err
}
