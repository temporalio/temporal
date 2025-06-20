package searchattribute

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// This test is to make sure all system search attributes have a corresponding
// column name in SQL DB.
// If this test is failing, make sure that you updated the SQL DB schemas, and
// you mapped the search attribute constant to the column name.
func TestValidateSqlDbSystemNameToColNameMap(t *testing.T) {
	s := assert.New(t)
	s.Contains(sqlDbSystemNameToColName, NamespaceID)
	for key := range system {
		s.Contains(sqlDbSystemNameToColName, key)
	}
}
