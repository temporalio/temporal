package timestamp

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TimePtr(t time.Time) *timestamppb.Timestamp {
	return timestamppb.New(t)
}

func TimeValue(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}

func TimeValuePtr(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	result := t.AsTime()
	return &result
}

func UnixOrZeroTime(nanos int64) time.Time {
	if nanos <= 0 {
		nanos = 0
	}

	return time.Unix(0, nanos).UTC()
}

func UnixOrZeroTimePtr(nanos int64) *timestamppb.Timestamp {
	return TimePtr(UnixOrZeroTime(nanos))
}

func TimeNowPtrUtcAddDuration(t time.Duration) *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC().Add(t))
}

func TimeNowPtrUtcAddSeconds(seconds int) *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC().Add(time.Second * time.Duration(seconds)))
}

func TimeNowPtrUtc() *timestamppb.Timestamp {
	return TimePtr(time.Now().UTC())
}
