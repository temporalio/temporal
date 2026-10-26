package update

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	updatepb "go.temporal.io/api/update/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestAdmittedUpdatesKeepArrivalOrderWithinOneClockTick(t *testing.T) {
	// Two admissions inside one clock tick carry the same admittedTime, so
	// only the admission sequence can keep them in order.
	first := newAdmitted("first", mustAny(t))
	second := newAdmitted("second", mustAny(t))
	second.admittedTime = first.admittedTime
	require.Less(t, first.admittedSeq, second.admittedSeq)

	// A later clock tick still wins over any sequence number.
	later := newAdmitted("later", mustAny(t))
	later.admittedTime = first.admittedTime.Add(time.Nanosecond)
	later.admittedSeq = 0
	require.Equal(t, -1, compareAdmission(first, later))
	require.Equal(t, -1, compareAdmission(first, second))
	require.Equal(t, 1, compareAdmission(second, first))
}

func mustAny(t *testing.T) *anypb.Any {
	t.Helper()
	request, err := anypb.New(&updatepb.Request{})
	require.NoError(t, err)
	return request
}
