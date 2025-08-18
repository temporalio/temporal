package dynamicconfig

import (
	"regexp"

	"github.com/mitchellh/mapstructure"
	"go.temporal.io/server/common/util"
)

var (
	MatchAnythingRE = regexp.MustCompile(".*")
	MatchNothingRE  = regexp.MustCompile(".^")
)

func ConvertWildcardStringListToRegexp(in any) (*regexp.Regexp, error) {
	// first convert raw value to list of strings
	var patterns []string
	if err := mapstructure.Decode(in, &patterns); err != nil {
		return nil, err
	}
	// then turn strings into regexp
	return util.WildCardStringsToRegexp(patterns)
}
