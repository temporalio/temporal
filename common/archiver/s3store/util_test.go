package s3store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructVisibilitySearchPrefix(t *testing.T) {
	t.Parallel()
	assert.Equal(
		t,
		constructVisibilitySearchPrefix(
			"path",
			"namespaceID",
		),
		"path/namespaceID/visibility",
	)
}

func TestConstructIndexedVisibilitySearchPrefix(t *testing.T) {
	t.Parallel()
	assert.Equal(
		t,
		constructIndexedVisibilitySearchPrefix(
			"/path",
			"namespaceID",
			"primaryIndexKey",
			"primaryIndexValue",
			"secondaryIndexType",
		),
		"path/namespaceID/visibility/primaryIndexKey/primaryIndexValue/secondaryIndexType",
	)
}
