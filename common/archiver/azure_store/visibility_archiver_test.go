package azure_store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/archiver"
)

func TestVisibilityValidateURI(t *testing.T) {
	v := &visibilityArchiver{}
	tests := []struct {
		uri      string
		expected error
	}{
		{
			uri:      "azblob://my-container/path",
			expected: nil,
		},
		{
			uri:      "s3://my-bucket/path",
			expected: archiver.ErrURISchemeMismatch,
		},
		{
			uri:      "azblob://",
			expected: archiver.ErrInvalidURI,
		},
	}

	for _, tt := range tests {
		uri, _ := archiver.NewURI(tt.uri)
		err := v.validateURI(uri)
		assert.Equal(t, tt.expected, err)
	}
}
