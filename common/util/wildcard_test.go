package util_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/util"
)

func TestWildCardStringToRegexp(t *testing.T) {
	re, err := util.WildCardStringToRegexp("a*z")
	require.NoError(t, err)
	require.Regexp(t, re, "az")
	require.Regexp(t, re, "abz")
	require.NotRegexp(t, re, "ab")

	_, err = util.WildCardStringToRegexp("")
	require.ErrorContains(t, err, "pattern cannot be empty")
}

func TestWildCardStringsToRegexp(t *testing.T) {
	re, err := util.WildCardStringsToRegexp([]string{"a*z", "b*d"})
	require.NoError(t, err)
	require.Regexp(t, re, "az")
	require.Regexp(t, re, "abz")
	require.NotRegexp(t, re, "ab")
	require.Regexp(t, re, "bd")
	require.Regexp(t, re, "bcd")
	require.NotRegexp(t, re, "bc")

	re, err = util.WildCardStringsToRegexp([]string{})
	require.NoError(t, err)
	require.NotRegexp(t, re, "a")
}
