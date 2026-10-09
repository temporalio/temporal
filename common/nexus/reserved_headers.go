package nexus

import (
	"slices"
	"strings"
)

// reservedHeaderPrefix is the Nexus header key prefix reserved for Temporal's own use.
const reservedHeaderPrefix = "temporal-"

// ReservedHeaderKeys returns the sorted keys of the given lower-cased header that use the reserved prefix.
func ReservedHeaderKeys(lowerCaseHeader map[string]string) []string {
	var keys []string
	for k := range lowerCaseHeader {
		if strings.HasPrefix(k, reservedHeaderPrefix) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
}
