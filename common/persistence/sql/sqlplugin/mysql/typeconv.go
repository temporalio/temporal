package mysql

import "time"

var (
	minMySQLDateTime = getMinMySQLDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to mysql datatypes
	DataConverter interface {
		ToMySQLDateTime(t time.Time) time.Time
		FromMySQLDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToMySQLDateTime converts to time to MySQL datetime
func (c *converter) ToMySQLDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minMySQLDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromMySQLDateTime converts mysql datetime and returns go time
func (c *converter) FromMySQLDateTime(t time.Time) time.Time {
	if t.Equal(minMySQLDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinMySQLDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
