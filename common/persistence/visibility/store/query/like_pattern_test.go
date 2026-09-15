package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertLikePatternToESWildcard(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		pattern string
		want    string
	}{
		{pattern: "", want: ""},
		{pattern: "foo", want: "foo"},
		{pattern: "%foo%", want: "*foo*"},
		{pattern: "foo%", want: "foo*"},
		{pattern: "%foo", want: "*foo"},
		{pattern: "f_o", want: "f?o"},
		{pattern: "%foo_bar%", want: "*foo?bar*"},
		{pattern: "foo*bar", want: `foo\*bar`},
		{pattern: "foo?bar", want: `foo\?bar`},
		{pattern: `foo\bar`, want: `foo\\bar`},
	}

	for _, tc := range testCases {
		t.Run(tc.pattern, func(t *testing.T) {
			assert.Equal(t, tc.want, ConvertLikePatternToESWildcard(tc.pattern))
		})
	}
}
