package query

import "strings"

// ConvertLikePatternToESWildcard translates a SQL LIKE pattern into an Elasticsearch
// wildcard query pattern: '%' becomes '*', '_' becomes '?', and any literal '*', '?'
// or '\' in the input is escaped with a backslash so it matches itself.
func ConvertLikePatternToESWildcard(pattern string) string {
	var sb strings.Builder
	sb.Grow(len(pattern))
	for _, r := range pattern {
		switch r {
		case '%':
			sb.WriteByte('*')
		case '_':
			sb.WriteByte('?')
		case '*', '?', '\\':
			sb.WriteByte('\\')
			sb.WriteRune(r)
		default:
			sb.WriteRune(r)
		}
	}
	return sb.String()
}
