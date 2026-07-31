package api

import (
	"strconv"
	"strings"

	"github.com/papercomputeco/tapes/pkg/cassette"
)

// DefaultContractVersions returns the tapes contracts this build of core
// serves.
//
// This is the API server's fact to hold, and it lives next to the handlers
// that serve the surface it names. It is deliberately not a constant in
// pkg/cassette: that package is the vocabulary a cassette uses to describe
// itself, and a cassette declaring "I read tapes v1" must not depend on a
// package that also asserts which core is running. The dependency goes one
// way — core reads a cassette's declaration and decides.
//
// It is a set rather than a single value so a deployment can serve a new
// contract while still admitting cassettes built against the previous one.
// That is the only way to roll a fleet of cassettes forward without a flag
// day, and it costs nothing to allow for now.
func DefaultContractVersions() []cassette.ContractVersion {
	return []cassette.ContractVersion{"v1"}
}

// resolveContractVersions applies the DefaultContractVersions() fallback and
// returns a copy the server can hold for its lifetime, so the set admission
// checks against and the set discovery advertises cannot drift apart.
func resolveContractVersions(configured []cassette.ContractVersion) []cassette.ContractVersion {
	if len(configured) == 0 {
		return DefaultContractVersions()
	}

	return append([]cassette.ContractVersion(nil), configured...)
}

// currentContractVersion returns the newest contract in versions — the single
// one the discovery document advertises as current.
//
// Newest is decided by numeric suffix rather than by position, so a config
// that lists its contracts out of order still advertises the right one, and
// rather than by string order, so v10 sorts after v9. Entries that are not
// well-formed are skipped; an empty or wholly malformed set advertises
// nothing, which is more honest than advertising a contract at random.
func currentContractVersion(versions []cassette.ContractVersion) cassette.ContractVersion {
	var current cassette.ContractVersion
	highest := 0
	for _, version := range versions {
		major, ok := contractMajor(version)
		if !ok || major <= highest {
			continue
		}
		highest, current = major, version
	}

	return current
}

// contractMajor parses the N out of a "vN" contract version.
func contractMajor(version cassette.ContractVersion) (int, bool) {
	digits, found := strings.CutPrefix(string(version), "v")
	if !found {
		return 0, false
	}
	major, err := strconv.Atoi(digits)
	if err != nil || major < 1 {
		return 0, false
	}

	return major, true
}
