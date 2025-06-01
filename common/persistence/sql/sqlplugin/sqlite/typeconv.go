package sqlite

import "time"

var (
	minSQLiteDateTime = getMinSQLiteDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to sqlite datatypes
	DataConverter interface {
		ToSQLiteDateTime(t time.Time) time.Time
		FromSQLiteDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToSQLiteDateTime converts to time to SQLite datetime
func (c *converter) ToSQLiteDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minSQLiteDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromSQLiteDateTime converts SQLite datetime and returns go time
func (c *converter) FromSQLiteDateTime(t time.Time) time.Time {
	if t.Equal(minSQLiteDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinSQLiteDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
