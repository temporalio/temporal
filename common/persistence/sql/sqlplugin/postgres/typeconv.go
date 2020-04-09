package postgres

import "time"

var localZone, _ = time.Now().Zone()
var localOffset = getLocalOffset()

type (
	// DataConverter defines the API for conversions to/from
	// go types to postgres datatypes
	// TODO https://github.com/uber/cadence/issues/2892
	// There are some reasons:
	//r application layer is not consistent with timezone: for example,
	// in some case we write timestamp with local timezone but when the time.Time
	// is converted from "JSON"(from paging token), the timezone is missing
	DataConverter interface {
		ToPostgresDateTime(t time.Time) time.Time
		FromPostgresDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToPostgresDateTime converts to time to Postgres datetime
func (c *converter) ToPostgresDateTime(t time.Time) time.Time {
	zn, _ := t.Zone()
	if zn != localZone {
		nano := t.UnixNano()
		t := time.Unix(0, nano)
		return t
	}
	return t
}

// FromPostgresDateTime converts postgres datetime and returns go time
func (c *converter) FromPostgresDateTime(t time.Time) time.Time {
	return t.Add(-localOffset)
}

func getLocalOffset() time.Duration {
	_, offsetSecs := time.Now().Zone()
	return time.Duration(offsetSecs) * time.Second
}
