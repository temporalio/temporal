package schema

import (
	"github.com/blang/semver/v4"
)

// cmpVersion compares two version strings
// returns 0 if a == b
// returns < 0 if a < b
// returns > 0 if a > b
func cmpVersion(a, b string) int {
	aParsed, _ := semver.ParseTolerant(a)
	bParsed, _ := semver.ParseTolerant(b)
	return aParsed.Compare(bParsed)
}
