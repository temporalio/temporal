package sqlplugin

import "strings"

func appendPrefix(prefix string, fields []string) []string {
	out := make([]string, len(fields))
	for i, field := range fields {
		out[i] = prefix + field
	}
	return out
}

func BuildNamedPlaceholder(fields ...string) string {
	return strings.Join(appendPrefix(":", fields), ", ")
}
