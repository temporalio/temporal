package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_BuildPutMappingBody(t *testing.T) {
	tests := []struct {
		root     string
		expected string
	}{
		{
			root:     "Attr",
			expected: "map[properties:map[Attr:map[properties:map[testKey:map[type:text]]]]]",
		},
		{
			root:     "",
			expected: "map[properties:map[testKey:map[type:text]]]",
		},
	}
	k := "testKey"
	v := "text"

	for _, test := range tests {
		require.Equal(t, test.expected, fmt.Sprintf("%v", buildPutMappingBody(test.root, k, v)))
	}
}
