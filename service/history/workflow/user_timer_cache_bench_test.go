package workflow

import (
	"fmt"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BenchmarkUserTimersLazyVsEager(b *testing.B) {
	const numTimers = 10000
	serializer := serialization.NewSerializer()
	logger := log.NewNoopLogger()

	blobs := make(map[string]*commonpb.DataBlob, numTimers)
	for i := 0; i < numTimers; i++ {
		timerID := fmt.Sprintf("timer-%05d", i)
		blob, err := serializer.TimerInfoToBlob(&persistencespb.TimerInfo{
			TimerId:        timerID,
			StartedEventId: int64(i),
			Version:        1,
			ExpiryTime:     timestamppb.New(time.Now().UTC().Add(time.Hour)),
		})
		if err != nil {
			b.Fatal(err)
		}
		blobs[timerID] = blob
	}

	b.Run("eager_decode_all_on_load", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c := newUserTimers(logger)
			c.seedBlobs(blobs)
			if got := len(c.all()); got != numTimers {
				b.Fatalf("expected %d timers, got %d", numTimers, got)
			}
		}
	})

	b.Run("lazy_seed_untouched_only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			c := newUserTimers(logger)
			c.seedBlobs(blobs)
			c.untouchedBlobs()
		}
	})

	b.Run("lazy_single_entry_access", func(b *testing.B) {
		b.ReportAllocs()
		// get() moves entries out of the owned blobs map, so every iteration
		// needs a fresh one-entry map
		blob := blobs["timer-04999"]
		for i := 0; i < b.N; i++ {
			c := newUserTimers(logger)
			c.seedBlobs(map[string]*commonpb.DataBlob{"timer-04999": blob})
			if _, ok := c.get("timer-04999"); !ok {
				b.Fatal("timer not found")
			}
		}
	})
}
