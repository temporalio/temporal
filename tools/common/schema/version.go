package schema

import (
	"fmt"
	"regexp"

	"github.com/blang/semver/v4"
)

// represents names of the form vx.x where x.x is a (major, minor) version pair
var versionStrRegex = regexp.MustCompile(`^v\d+(\.\d+)?$`)

// represents names of the form x.x where minor version is always single digit
var versionNumRegex = regexp.MustCompile(`^\d+(\.\d+)?$`)

// cmpVersion compares two version strings
// returns 0 if a == b
// returns < 0 if a < b
// returns > 0 if a > b
func cmpVersion(a, b string) int {
	aParsed, _ := semver.ParseTolerant(a)
	bParsed, _ := semver.ParseTolerant(b)
	return aParsed.Compare(bParsed)
}

// parseValidateVersion validates that the given input conforms to either of vx.x or x.x and
// returns x.x on success
func parseValidateVersion(ver string) (string, error) {
	if len(ver) == 0 {
		return "", fmt.Errorf("version is empty")
	}
	if versionStrRegex.MatchString(ver) {
		return ver[1:], nil
	}
	if !versionNumRegex.MatchString(ver) {
		return "", fmt.Errorf("invalid version, expected format is x.x")
	}
	return ver, nil
}
