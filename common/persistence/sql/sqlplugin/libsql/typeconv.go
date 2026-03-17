package libsql

import "time"

var (
	minSQLiteDateTime = getMinSQLiteDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// Go types and database column types (SQLite-compatible datetime).
	DataConverter interface {
		ToSQLiteDateTime(t time.Time) time.Time
		FromSQLiteDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToSQLiteDateTime converts Go time to database datetime (SQLite-compatible format).
func (c *converter) ToSQLiteDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minSQLiteDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromSQLiteDateTime converts database datetime back to Go time.
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
