package timestamp

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reUnitless = regexp.MustCompile(`^(\d+(\.\d*)?|(\.\d+))$`)
	reDays     = regexp.MustCompile(`(\d+(\.\d*)?|(\.\d+))d`)

	errInvalidDuration        = errors.New("invalid duration")
	errInvalidDurationHours   = errors.New("invalid duration: hours must be a positive number")
	errInvalidDurationMinutes = errors.New("invalid duration: minutes must be from 0 to 59")
	errInvalidDurationSeconds = errors.New("invalid duration: seconds must be from 0 to 59")
)

// ParseDuration is like time.ParseDuration, but supports unit "d" for days
// (always interpreted as exactly 24 hours).
func ParseDuration(s string) (time.Duration, error) {
	s = reDays.ReplaceAllStringFunc(s, func(v string) string {
		fv, err := strconv.ParseFloat(strings.TrimSuffix(v, "d"), 64)
		if err != nil {
			return v // will cause time.ParseDuration to return an error
		}
		return fmt.Sprintf("%fh", 24*fv)
	})
	return time.ParseDuration(s)
}

// ParseDurationDefaultDays is like time.ParseDuration, but supports unit "d"
// for days (always interpreted as exactly 24 hours), and also supports
// unit-less numbers, which are interpreted as days.
func ParseDurationDefaultDays(s string) (time.Duration, error) {
	if reUnitless.MatchString(s) {
		s += "d"
	}
	return ParseDuration(s)
}

// ParseDurationDefaultSeconds is like time.ParseDuration, but supports unit "d"
// for days (always interpreted as exactly 24 hours), and also supports
// unit-less numbers, which are interpreted as seconds.
func ParseDurationDefaultSeconds(s string) (time.Duration, error) {
	if reUnitless.MatchString(s) {
		s += "s"
	}
	return ParseDuration(s)
}

func ParseHHMMSSDuration(d string) (time.Duration, error) {
	var hours, minutes, seconds time.Duration
	_, err := fmt.Sscanf(d, "%d:%d:%d", &hours, &minutes, &seconds)
	if err != nil {
		return 0, errInvalidDuration
	}
	if hours < 0 {
		return 0, errInvalidDurationHours
	}
	if minutes < 0 || minutes > 59 {
		return 0, errInvalidDurationMinutes
	}
	if seconds < 0 || seconds > 59 {
		return 0, errInvalidDurationSeconds
	}

	return hours*time.Hour + minutes*time.Minute + seconds*time.Second, nil
}
