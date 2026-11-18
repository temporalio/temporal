package historydlq

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	historydlqpb "go.temporal.io/server/chasm/lib/historydlq/gen/historydlqpb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StoredMessage struct {
	Ref                MessageRef
	SourceTaskIdentity TaskIdentity
	Task               *commonspb.HistoryTask
}

type Entry struct {
	chasm.UnimplementedComponent
	*historydlqpb.EntryState
	Task chasm.Field[*commonspb.HistoryTask]
}

func newEntry(ctx chasm.MutableContext, req EnqueueRequest, enqueuedAt time.Time) (*Entry, error) {
	encoding := req.Task.GetBlob().GetEncodingType()
	if req.Task.GetShardId() != req.SourceTaskIdentity.SourceShardID || len(req.Task.GetBlob().GetData()) == 0 ||
		(encoding != enumspb.ENCODING_TYPE_PROTO3 && encoding != enumspb.ENCODING_TYPE_JSON) {
		return nil, serviceerror.NewInvalidArgument("history DLQ requires a nonempty supported task with the source shard")
	}
	id, err := messageID(req.Queue, req.SourceTaskIdentity, enqueuedAt)
	if err != nil {
		return nil, err
	}
	return &Entry{
		EntryState: &historydlqpb.EntryState{
			SourceTaskIdentity: taskIdentityToEntryProto(req.SourceTaskIdentity),
			MessageId:          id,
			EnqueuedAt:         timestamppb.New(enqueuedAt),
			PayloadSizeBytes:   int64(proto.Size(req.Task)),
		},
		Task: chasm.NewDataField(ctx, common.CloneProto(req.Task)),
	}, nil
}

func (e *Entry) Read(ctx chasm.Context, ref MessageRef, sourceShardID int32) (StoredMessage, error) {
	stored := StoredMessage{
		Ref:                ref,
		SourceTaskIdentity: entryTaskIdentityProtoToIdentity(e.GetSourceTaskIdentity(), sourceShardID),
	}
	task, ok := e.Task.TryGet(ctx)
	if !ok {
		return StoredMessage{}, serviceerror.NewDataLoss("history DLQ entry is missing its task")
	}
	stored.Task = common.CloneProto(task)
	return stored, nil
}

func (e *Entry) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
