package historydlq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
)

func TestEntryReadMissingTask(t *testing.T) {
	t.Parallel()
	entry := &Entry{EntryState: &historydlqpb.EntryState{}}
	_, err := entry.Read(&chasm.MockContext{}, MessageRef{}, 7)
	require.ErrorAs(t, err, new(*serviceerror.DataLoss))
}

func TestEntryRoundTrip(t *testing.T) {
	t.Parallel()
	for _, encoding := range []enumspb.EncodingType{enumspb.ENCODING_TYPE_PROTO3, enumspb.ENCODING_TYPE_JSON} {
		t.Run(encoding.String(), func(t *testing.T) {
			t.Parallel()
			ctx := &chasm.MockMutableContext{}
			req := testEnqueueRequest()
			req.Task.Blob.EncodingType = encoding
			now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
			entry, err := newEntry(ctx, req, now)
			require.NoError(t, err)
			require.Equal(t, now, entry.GetEnqueuedAt().AsTime())
			require.Equal(t, int64(proto.Size(req.Task)), entry.GetPayloadSizeBytes())
			ref := MessageRef{BusinessID: "segment-business", RunID: "segment-run", Queue: req.Queue, EnqueuedAt: now, MessageID: entry.GetMessageId(), EntryKey: "entry"}
			stored, err := entry.Read(ctx, ref, req.SourceTaskIdentity.SourceShardID)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, req.Task, stored.Task)
			stored.Task.Blob.Data[0] = 'y'
			req.Task.Blob.Data[0] = 'x'
			again, err := entry.Read(ctx, ref, req.SourceTaskIdentity.SourceShardID)
			require.NoError(t, err)
			require.Equal(t, []byte("task payload"), again.Task.Blob.Data)
			again.Task = nil
			require.Equal(t, StoredMessage{Ref: ref, SourceTaskIdentity: req.SourceTaskIdentity}, again)
		})
	}
}
