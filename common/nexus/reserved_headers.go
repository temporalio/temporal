package nexus

import (
	"slices"
	"strings"
)

// ReservedHeaderPrefix is the Nexus header key prefix reserved for Temporal's own use.
const ReservedHeaderPrefix = "temporal-"

// IsReservedHeader reports whether the given lower-cased header key uses the reserved prefix.
func IsReservedHeader(lowerKey string) bool {
	return strings.HasPrefix(lowerKey, ReservedHeaderPrefix)
}

// ReservedHeaderKeys returns the sorted keys of the given lower-cased header that use the reserved prefix.
func ReservedHeaderKeys(lowerCaseHeader map[string]string) []string {
	var keys []string
	for k := range lowerCaseHeader {
		if IsReservedHeader(k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}
