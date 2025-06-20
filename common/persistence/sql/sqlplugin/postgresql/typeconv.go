package postgresql

import "time"

var (
	minPostgreSQLDateTime = getMinPostgreSQLDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// go types to mysql datatypes
	DataConverter interface {
		ToPostgreSQLDateTime(t time.Time) time.Time
		FromPostgreSQLDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToPostgreSQLDateTime converts to time to PostgreSQL datetime
func (c *converter) ToPostgreSQLDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minPostgreSQLDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromPostgreSQLDateTime converts postgresql datetime and returns go time
func (c *converter) FromPostgreSQLDateTime(t time.Time) time.Time {
	// NOTE: PostgreSQL will preserve the location of time in a
	//  weird way, here need to call UTC to remove the time location
	if t.Equal(minPostgreSQLDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinPostgreSQLDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
