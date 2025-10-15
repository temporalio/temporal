package nexusrpc

import (
	"fmt"
	"strings"
	"time"
)

const (
	maxTimestampSeconds = 253402300799
	minTimestampSeconds = -62135596800
)

// marshalTimestamp marshals a Time instance into string. Uses RFC 3339, where generated output will be Z-normalized and
// uses 0, 3, 6 or 9 fractional digits.
// Copied from https://github.com/protocolbuffers/protobuf-go/blob/0b2c87d84c27802dae7248480444e22421ba577d/encoding/protojson/well_known_types.go#L749C1-L826C2
func marshalTimestamp(t time.Time) string {
	x := t.UTC().Format("2006-01-02T15:04:05.000000000")
	x = strings.TrimSuffix(x, "000")
	x = strings.TrimSuffix(x, "000")
	x = strings.TrimSuffix(x, ".000")
	return x + "Z"
}

// unmarshalTimestamp unmarshals a string into a Time instance. Uses RFC 3339, with some extra validation to ensure that
// seconds and subseconds are with an expected range.
// Copied from https://github.com/protocolbuffers/protobuf-go/blob/0b2c87d84c27802dae7248480444e22421ba577d/encoding/protojson/well_known_types.go#L749C1-L826C2
func unmarshalTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return t, err
	}
	// Validate seconds.
	secs := t.Unix()
	if secs < minTimestampSeconds || secs > maxTimestampSeconds {
		return t, fmt.Errorf("second value out of range: %v", secs)
	}
	// Validate subseconds.
	i := strings.LastIndexByte(s, '.')  // start of subsecond field
	j := strings.LastIndexAny(s, "Z-+") // start of timezone field
	if i >= 0 && j >= i && j-i > len(".999999999") {
		return t, fmt.Errorf("invalid subsecond value %v", s)
	}
	return t, nil
}
