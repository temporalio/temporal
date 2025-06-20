package schema

import (
	"github.com/blang/semver/v4"
)

// normalizeVersionString take a valid semver string and returns the input as-is with the 'v' prefix removed if present
func normalizeVersionString(ver string) (string, error) {
	if _, err := semver.ParseTolerant(ver); err != nil {
		return "", err
	}
	if ver[0] == 'v' {
		return ver[1:], nil
	}
	return ver, nil
}
