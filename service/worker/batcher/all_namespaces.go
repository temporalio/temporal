package batcher

import (
	"errors"
	"fmt"
	"strings"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/sqlquery"
)

// ValidateAllNamespacesQuery rejects a visibility query that cannot be used across all namespaces:
// a blank query would select every execution in the cluster, and custom search attributes, including
// raw field names such as Keyword01, mean different things in different namespaces.
func ValidateAllNamespacesQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return errors.New("a query across all namespaces requires a visibility query")
	}
	statement, err := sqlparser.Parse(fmt.Sprintf(sqlquery.QueryTemplate, query))
	if err != nil {
		return fmt.Errorf("invalid query %q: %w", query, err)
	}
	var customSAs []string
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			name := strings.ReplaceAll(sqlparser.String(col), "`", "")
			if !isSystemOrPredefinedSearchAttribute(name) {
				customSAs = append(customSAs, name)
			}
		}
		return true, nil
	}, statement)
	if len(customSAs) > 0 {
		return fmt.Errorf("a query across all namespaces cannot filter on custom search attributes; only system and predefined search attributes are supported, got: %s",
			strings.Join(customSAs, ", "))
	}
	return nil
}

// isSystemOrPredefinedSearchAttribute accepts canonical names and reserved aliases.
// Unprefixed aliases can collide with custom fields, which take precedence during query resolution.
func isSystemOrPredefinedSearchAttribute(name string) bool {
	if !sadefs.IsMappable(name) {
		return true
	}
	trimmed, prefixed := strings.CutPrefix(name, sadefs.ReservedPrefix)
	return prefixed && (trimmed == sadefs.ScheduleID || !sadefs.IsMappable(trimmed))
}
