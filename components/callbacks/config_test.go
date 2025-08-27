package callbacks

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_addressPatternToRegexp(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		want    string
	}{
		{name: "empty", pattern: "", want: "^$"},
		{name: "no_wildcard", pattern: "foo", want: "^foo$"},
		{name: "single_wildcard_only", pattern: "*", want: "^.*$"},
		{name: "leading_wildcard", pattern: "*foo", want: "^.*foo$"},
		{name: "trailing_wildcard", pattern: "foo*", want: "^foo.*$"},
		{name: "surrounded_wildcard", pattern: "*foo*", want: "^.*foo.*$"},
		{name: "middle_wildcard", pattern: "foo*bar", want: "^foo.*bar$"},
		{name: "literal_dots_around_wildcard", pattern: "foo.*bar", want: "^foo\\..*bar$"},
		{name: "prefix_subdomain", pattern: "prefix.*.domain", want: "^prefix\\..*\\.domain$"},
		{name: "leading_any_subdomain", pattern: "*.example.com", want: "^.*\\.example\\.com$"},
		{name: "host_with_port", pattern: "api.example.com:8080", want: "^api\\.example\\.com:8080$"},
		{name: "consecutive_wildcards", pattern: "a**b", want: "^a.*.*b$"},
		{name: "triple_wildcards", pattern: "a***b", want: "^a.*.*.*b$"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := addressPatternToRegexp(test.pattern)
			require.Equal(t, test.want, got)
			_, err := regexp.Compile(got)
			require.NoError(t, err)
		})
	}
}
