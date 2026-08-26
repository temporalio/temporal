package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newTestUserTimers(t *testing.T) (*userTimers, func(string) *commonpb.DataBlob) {
	serializer := serialization.NewSerializer()
	nextEventID := int64(10)
	return newUserTimers(log.NewNoopLogger()), func(timerID string) *commonpb.DataBlob {
		startEventID := nextEventID
		nextEventID++
		blob, err := serializer.TimerInfoToBlob(&persistencespb.TimerInfo{
			TimerId:        timerID,
			StartedEventId: startEventID,
			Version:        1,
			ExpiryTime:     timestamppb.New(time.Now().UTC().Add(time.Hour)),
		})
		require.NoError(t, err)
		return blob
	}
}

func TestUserTimersLazyDecode(t *testing.T) {
	container, encode := newTestUserTimers(t)
	container.seedBlobs(map[string]*commonpb.DataBlob{
		"t1": encode("t1"),
		"t2": encode("t2"),
	})

	// seeding must not decode anything
	require.Nil(t, container.typed)
	require.Equal(t, 2, container.len())
	require.True(t, container.has("t1"))
	require.False(t, container.has("t0"))
	require.Nil(t, container.eventIDToID)

	info, ok := container.get("t1")
	require.True(t, ok)
	require.Equal(t, "t1", info.GetTimerId())
	require.Len(t, container.typed, 1)

	// decoded entries stay cached, untouched ones stay encoded
	require.Equal(t, 2, container.len())
	require.NotNil(t, container.blobs["t2"])
}

func TestUserTimersGetByEventIDDecodesAll(t *testing.T) {
	container, encode := newTestUserTimers(t)
	container.seedBlobs(map[string]*commonpb.DataBlob{
		"t1": encode("t1"),
		"t2": encode("t2"),
	})

	info, ok := container.getByEventID(10)
	require.True(t, ok)
	require.Equal(t, "t1", info.GetTimerId())

	// reverse lookup over fully encoded state as well
	freshContainer, freshEncode := newTestUserTimers(t)
	freshContainer.seedBlobs(map[string]*commonpb.DataBlob{"t3": freshEncode("t3")})
	info, ok = freshContainer.getByEventID(10)
	require.True(t, ok)
	require.Equal(t, "t3", info.GetTimerId())
	require.Nil(t, freshContainer.blobs)

	_, ok = freshContainer.getByEventID(11)
	require.False(t, ok)
}

func TestUserTimersPutOverridesEncodedEntry(t *testing.T) {
	container, encode := newTestUserTimers(t)
	container.seedBlobs(map[string]*commonpb.DataBlob{"t1": encode("t1")})

	updated := &persistencespb.TimerInfo{TimerId: "t1", StartedEventId: 20}
	container.put(updated)

	require.Nil(t, container.blobs["t1"])
	require.Same(t, updated, container.typed["t1"])
	require.Equal(t, 1, container.len())

	got, ok := container.getByEventID(20)
	require.True(t, ok)
	require.Same(t, updated, got)
	// the old start event ID must no longer resolve
	_, ok = container.getByEventID(10)
	require.False(t, ok)
}

func TestUserTimersDeleteEncodedEntry(t *testing.T) {
	container, encode := newTestUserTimers(t)
	container.seedBlobs(map[string]*commonpb.DataBlob{"t1": encode("t1"), "t2": encode("t2")})

	info, ok := container.delete("t1")
	require.True(t, ok)
	require.Equal(t, "t1", info.GetTimerId())
	require.Equal(t, 1, container.len())
	require.False(t, container.has("t1"))
	_, ok = container.getByEventID(10)
	require.False(t, ok)

	_, ok = container.delete("missing")
	require.False(t, ok)
}

func TestUserTimersCorruptBlobIsDropped(t *testing.T) {
	container := newUserTimers(log.NewNoopLogger())
	container.seedBlobs(map[string]*commonpb.DataBlob{
		"bad": {EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte{0xFF, 0x00}},
	})

	_, ok := container.get("bad")
	require.False(t, ok)
	require.Equal(t, 0, container.len())

	_, ok = container.getByEventID(10)
	require.False(t, ok)
}

func TestUserTimersUntouchedBlobsOwnershipTransfer(t *testing.T) {
	container, encode := newTestUserTimers(t)
	container.seedBlobs(map[string]*commonpb.DataBlob{
		"t1": encode("t1"),
		"t2": encode("t2"),
	})
	_, ok := container.get("t1")
	require.True(t, ok)

	untouched := container.untouchedBlobs()
	require.Len(t, untouched, 1)
	require.Contains(t, untouched, "t2")
	require.Nil(t, container.blobs)
	require.Equal(t, 1, container.len())
}
