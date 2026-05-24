package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/metrics"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/testvars"
)

// setupStreamForTest constructs a fresh Stream root component wired up
// with a chasm Node for the test.  Returns the stream, a MutableContext
// for further transitions, and the underlying Node.
func setupStreamForTest(t *testing.T) (*stream.Stream, chasm.MutableContext) {
	t.Helper()

	nodeBackend := &chasm.MockNodeBackend{}
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(stream.NewNilLibrary()))

	tv := testvars.New(t)
	nodeBackend.HandleNextTransitionCount = func() int64 { return 2 }
	nodeBackend.HandleGetCurrentVersion = func() int64 { return 1 }
	nodeBackend.HandleGetWorkflowKey = tv.Any().WorkflowKey
	nodeBackend.HandleIsWorkflow = func() bool { return false }
	nodeBackend.HandleCurrentVersionedTransition = func() *persistencespb.VersionedTransition {
		return &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          1,
		}
	}

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())

	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), node)

	s, err := stream.NewStream(ctx, &streampb.CreateStreamRequest{
		NamespaceId: "ns-1",
		StreamId:    "stream-1",
		CreatedBy:   "test-user",
	})
	require.NoError(t, err)

	require.NoError(t, node.SetRootComponent(s))
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	ctx = chasm.NewMutableContext(context.Background(), node)
	return s, ctx
}

// Spec correspondence: PreparePublish action sets first_offset =
// head_offset and reserves [first, first+n) without advancing head.
func TestPreparePublishReservesOffsetsWithoutAdvancingHead(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	require.Equal(t, int64(0), s.StreamState.HeadOffset)

	out, err := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1",
		Sequence:    1,
		ItemCount:   3,
		PayloadHash: []byte("h"),
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), out.FirstOffset)
	require.Equal(t, int32(3), out.ItemCount)
	require.False(t, out.IsDedupReplay)

	// Head must NOT have moved yet — only commit advances head.
	require.Equal(t, int64(0), s.StreamState.HeadOffset)
	require.Len(t, s.Inflights, 1)
}

// Spec correspondence: PreparePublish allocates monotone txn_ids.  The
// chasm engine guarantees stream-global monotonicity across owner_epoch
// bumps (see exactly-once.html Ownership-epoch fence invariant 2).
func TestPreparePublishAllocatesMonotoneTxnIDs(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	a, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h1"),
	})
	b, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-2", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h2"),
	})
	require.Less(t, a.TxnID, b.TxnID)
}

// Spec correspondence: ExactlyOnce + the design's "successful publish
// appears at exactly one offset regardless of retry."  Full publish
// cycle: Prepare → Commit advances head and writes pub_state.
func TestSinglePublishCommitsAndAdvancesHead(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, err := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 5, PayloadHash: []byte("h"),
	})
	require.NoError(t, err)

	out, err := s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID:                prep.TxnID,
		ProofOfWriteVerified: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), out.FirstOffset)
	require.Equal(t, int32(5), out.ItemCount)

	require.Equal(t, int64(5), s.StreamState.HeadOffset)
	require.Equal(t, prep.TxnID, s.StreamState.CommittedTxnId)
	require.Empty(t, s.Inflights)
	require.Len(t, s.Publishers, 1)
}

// Spec correspondence: dedup-replay short-circuit.  A retry with the
// same (publisher_id, sequence) returns the prior outcome.
func TestPrepareDedupReplayReturnsPriorOutcome(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 3, PayloadHash: []byte("h"),
	})
	_, _ = s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: true,
	})

	// Replay with same (publisher_id, sequence): returns the SAME
	// offset, marks IsDedupReplay, does not advance head further.
	replay, err := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 3, PayloadHash: []byte("h"),
	})
	require.NoError(t, err)
	require.True(t, replay.IsDedupReplay)
	require.Equal(t, int64(0), replay.FirstOffset)
	require.Equal(t, int32(3), replay.ItemCount)
	require.Equal(t, int64(3), s.StreamState.HeadOffset)
}

// Spec correspondence: MonotonicSeq.  Sequence regression is rejected.
func TestPreparePublishRejectsSeqRegression(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 5, ItemCount: 1, PayloadHash: []byte("h"),
	})
	_, _ = s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: true,
	})

	_, err := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 3, ItemCount: 1, PayloadHash: []byte("h"),
	})
	require.ErrorIs(t, err, stream.ErrSeqRegression)
}

// Spec correspondence: Close action.  After Close, mutators reject.
func TestCloseRejectsSubsequentMutators(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	_, closeErr := s.Close(ctx, stream.CloseInput{
		ClosedBy:    "tester",
		CloseReason: streampb.STREAM_CLOSE_REASON_EXPLICIT,
	})
	require.NoError(t, closeErr)

	_, err := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h"),
	})
	require.ErrorIs(t, err, stream.ErrStreamClosed)

	_, err = s.Truncate(ctx, stream.TruncateInput{UpToOffset: 0})
	require.ErrorIs(t, err, stream.ErrStreamClosed)
}

// Spec correspondence: PublishAbortedByClose.  In-flight publishes
// prepared before Close cannot commit; the in-band abort cleanup fires.
func TestCommitAfterCloseAborts(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h"),
	})
	_, closeErr := s.Close(ctx, stream.CloseInput{
		ClosedBy:    "tester",
		CloseReason: streampb.STREAM_CLOSE_REASON_EXPLICIT,
	})
	require.NoError(t, closeErr)

	_, err := s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: true,
	})
	require.ErrorIs(t, err, stream.ErrPublishAbortedClose)
	// Inflight removed by the abort cleanup.
	require.Empty(t, s.Inflights)
}

// Spec correspondence: TruncateBeyondHead guard.
func TestTruncateBeyondHeadRejects(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	_, err := s.Truncate(ctx, stream.TruncateInput{UpToOffset: 100})
	require.ErrorIs(t, err, stream.ErrTruncateBeyondHead)
}

// Spec correspondence: TruncateBlockedBySubscriber.  Active subscriber
// pin holds; force=true bypasses.
func TestTruncateRespectsAndForceClosesSubscribers(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	// Publish 5 items so head=5.
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 5, PayloadHash: []byte("h"),
	})
	_, _ = s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: true,
	})

	// Subscribe at offset 0.
	s.Subscriptions["sub-1"] = chasm.NewComponentField(ctx, &stream.Subscription{
		SubscriptionState: &streampb.SubscriptionState{
			SubscriberId: "sub-1",
			Cursor:       2,
		},
	})

	// Truncate to 3 (above the subscriber's cursor of 2) → blocked.
	_, err := s.Truncate(ctx, stream.TruncateInput{UpToOffset: 3})
	require.ErrorIs(t, err, stream.ErrTruncateBlockedBySub)

	// Force = true: subscriber gets closed, truncate succeeds.
	out, err := s.Truncate(ctx, stream.TruncateInput{UpToOffset: 3, Force: true})
	require.NoError(t, err)
	require.Equal(t, int32(1), out.ForceTruncatedSubscriptionCount)
	require.Empty(t, s.Subscriptions)
	require.Equal(t, int64(3), s.StreamState.BaseOffset)
}

// Spec correspondence: PublishAbortedByOwnerChange (the ownership-epoch
// fence).  An inflight whose prep_epoch lags owner_epoch aborts at commit.
func TestCommitAbortsOnOwnerEpochMismatch(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h"),
	})

	// Simulate ownership transfer between Prepare and Commit.
	s.StreamState.OwnerEpoch = 1

	_, err := s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: true,
	})
	require.ErrorIs(t, err, stream.ErrPublishAbortedOwner)
	require.Empty(t, s.Inflights)
	// Head must NOT have advanced.
	require.Equal(t, int64(0), s.StreamState.HeadOffset)
}

// Spec correspondence: CommittableInflightDisjoint — two prepares for
// the same offset race, only one wins commit (the second hits the CAS
// guard and aborts with OrderingGap).
func TestConcurrentPreparesOnlyOneCommits(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	a, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h1"),
	})
	b, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-2", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h2"),
	})
	// Both reserved first_offset = 0 — second-to-commit must fail CAS.

	_, err := s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: a.TxnID, ProofOfWriteVerified: true,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), s.StreamState.HeadOffset)

	_, err = s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: b.TxnID, ProofOfWriteVerified: true,
	})
	require.ErrorIs(t, err, stream.ErrOrderingGap)
	require.Empty(t, s.Inflights)
}

// Spec correspondence: CommitPublish proof-of-write guard.  When the
// caller passes ProofOfWriteVerified=false, the commit aborts with
// SegmentsMissing.
func TestCommitWithoutProofOfWriteAborts(t *testing.T) {
	s, ctx := setupStreamForTest(t)
	prep, _ := s.PreparePublish(ctx, stream.PreparePublishInput{
		PublisherID: "pub-1", Sequence: 1, ItemCount: 1, PayloadHash: []byte("h"),
	})
	_, err := s.CommitPublish(ctx, stream.CommitPublishInput{
		TxnID: prep.TxnID, ProofOfWriteVerified: false,
	})
	require.ErrorIs(t, err, stream.ErrSegmentsMissing)
	require.Empty(t, s.Inflights)
}
