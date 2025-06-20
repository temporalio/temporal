package timestamp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ParseDurationSuite struct {
	suite.Suite
}

func TestParseDurationSuite(t *testing.T) {
	suite.Run(t, new(ParseDurationSuite))
}

func (s *ParseDurationSuite) TestParseDuration() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"1h", time.Hour},
		{"3m30s", 3*time.Minute + 30*time.Second},
		{"1d", 24 * time.Hour},
		{"3d", 3 * 24 * time.Hour},
		{"5d6h15m", 5*24*time.Hour + 6*time.Hour + 15*time.Minute},
		{"5.25d15m", 5*24*time.Hour + 6*time.Hour + 15*time.Minute},
		{".5d", 12 * time.Hour},
		{"-10d12.25h", -(10*24*time.Hour + 12*time.Hour + 15*time.Minute)},
		{"3m2h1d", 3*time.Minute + 2*time.Hour + 1*24*time.Hour},
		{"8m7h6d5d4h3m", 8*time.Minute + 7*time.Hour + 6*24*time.Hour + 5*24*time.Hour + 4*time.Hour + 3*time.Minute},
		{"7", -1},         // error
		{"", -1},          // error
		{"10000000h", -1}, // error out of bounds
	} {
		got, err := ParseDuration(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}

func (s *ParseDurationSuite) TestParseDurationDefaultDays() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"3m30s", 3*time.Minute + 30*time.Second},
		{"7", 7 * 24 * time.Hour},
		{"7.5", 7*24*time.Hour + 12*time.Hour},
		{".75", 18 * time.Hour},
		{"2.75", 2*24*time.Hour + 18*time.Hour},
		{"", -1}, // error
	} {
		got, err := ParseDurationDefaultDays(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}

func (s *ParseDurationSuite) TestParseDurationDefaultSeconds() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"3m30s", 3*time.Minute + 30*time.Second},
		{"7", 7 * time.Second},
		{"", -1}, // error
	} {
		got, err := ParseDurationDefaultSeconds(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}

func (s *ParseDurationSuite) TestParseHHMMSSDuration() {
	for _, c := range []struct {
		input    string
		expected time.Duration // -1 means error
	}{
		{"1:00:00", 1 * time.Hour},
		{"123:05:10", 123*time.Hour + 5*time.Minute + 10*time.Second},
		{"00:05:10", 5*time.Minute + 10*time.Second},
		{"-12:05:10", -1},
		{"12:61:10", -1},
		{"12:05:61", -1},
		{"", -1},
	} {
		got, err := ParseHHMMSSDuration(c.input)
		if c.expected == -1 {
			s.Error(err)
		} else {
			s.Equal(c.expected, got)
		}
	}
}
