package schema

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
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

	aMajor, aMinor, _ := parseVersion(a)
	bMajor, bMinor, _ := parseVersion(b)

	if aMajor != bMajor {
		return aMajor - bMajor
	}

	return aMinor - bMinor
}

// parseVersion parses a version string and
// returns the major, minor version pair
func parseVersion(ver string) (major int, minor int, err error) {

	if len(ver) == 0 {
		return
	}

	vals := strings.Split(ver, ".")
	if len(vals) == 0 { // Split returns slice of size=1 on empty string
		return major, minor, nil
	}

	if len(vals) > 0 {
		major, err = strconv.Atoi(vals[0])
		if err != nil {
			return
		}
	}

	if len(vals) > 1 {
		minor, err = strconv.Atoi(vals[1])
		if err != nil {
			return
		}
	}

	return
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

// getExpectedVersion gets the latest version from the schema directory
func getExpectedVersion(dir string) (string, error) {
	subdirs, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var result string
	for _, subdir := range subdirs {
		if !subdir.IsDir() {
			continue
		}
		dirname := subdir.Name()
		if !versionStrRegex.MatchString(dirname) {
			continue
		}
		ver := dirToVersion(dirname)
		if len(result) == 0 || cmpVersion(ver, result) > 0 {
			result = ver
		}
	}
	if len(result) == 0 {
		return "", fmt.Errorf("no valid schemas found in dir: %s", dir)
	}
	return result, nil
}
