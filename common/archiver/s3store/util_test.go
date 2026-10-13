package s3store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructVisibilitySearchPrefix(t *testing.T) {
	t.Parallel()
	assert.Equal(
		t,
		"path/namespaceID/visibility", constructVisibilitySearchPrefix(
			"path",
			"namespaceID",
		),
	)
}

func TestConstructIndexedVisibilitySearchPrefix(t *testing.T) {
	t.Parallel()
	assert.Equal(
		t,
		"path/namespaceID/visibility/primaryIndexKey/primaryIndexValue/secondaryIndexType", constructIndexedVisibilitySearchPrefix(
			"/path",
			"namespaceID",
			"primaryIndexKey",
			"primaryIndexValue",
			"secondaryIndexType",
		),
	)
}
