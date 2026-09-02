package mssql

import "time"

var (
	minMSSQLDateTime = getMinMSSQLDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to mssql datatypes
	DataConverter interface {
		ToMSSQLDateTime(t time.Time) time.Time
		FromMSSQLDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToMSSQLDateTime converts time to MSSQL datetime. Values are stored in
// DATETIME2(6) columns; go-mssqldb encodes time.Time parameters as
// datetimeoffset, which compares correctly against DATETIME2 because every
// value in this plugin is UTC and truncated to microsecond precision.
func (c *converter) ToMSSQLDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minMSSQLDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromMSSQLDateTime converts mssql datetime and returns go time
func (c *converter) FromMSSQLDateTime(t time.Time) time.Time {
	if t.Equal(minMSSQLDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinMSSQLDateTime() time.Time {
	// Same sentinel the mysql/postgresql plugins use for zero time.Time;
	// well within the DATETIME2 range (0001-01-01 .. 9999-12-31).
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
