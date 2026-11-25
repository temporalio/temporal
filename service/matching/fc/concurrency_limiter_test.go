package fc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

const (
	testNsID   = namespace.ID("nsid")
	testConKey = "conkey"
)

type testClient struct {
	fcpb.ConcurrencyServiceClient
	batch func(context.Context, *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error)
	wait  func(context.Context, *fcpb.ConcurrencyServiceWaitRequest) (*fcpb.ConcurrencyServiceWaitResponse, error)
}

func (c *testClient) Batch(ctx context.Context, req *fcpb.ConcurrencyServiceBatchRequest, _ ...grpc.CallOption) (*fcpb.ConcurrencyServiceBatchResponse, error) {
	return c.batch(ctx, req)
}

func (c *testClient) Wait(ctx context.Context, req *fcpb.ConcurrencyServiceWaitRequest, _ ...grpc.CallOption) (*fcpb.ConcurrencyServiceWaitResponse, error) {
	return c.wait(ctx, req)
}

func TestOptimisticConcurrencyReserve(t *testing.T) {
	for _, allow := range []bool{false, true} {
		name := "blocked"
		if allow {
			name = "allowed"
		}
		t.Run(name, func(t *testing.T) {
			batches := 0
			client := &testClient{batch: func(_ context.Context, req *fcpb.ConcurrencyServiceBatchRequest) (*fcpb.ConcurrencyServiceBatchResponse, error) {
				batches++
				res := &fcpb.ConcurrencyServiceBatchResponse{}
				if len(req.ReserveSlots) > 0 {
					res.ReserveSuccess = []bool{allow}
				}
				if len(req.CommitSlots) > 0 {
					res.CommitSuccess = []bool{true}
				}
				return res, nil
			}}
			r := NewReadiness(client, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
			t.Cleanup(r.Stop)
			task := &testTask{limiters: []Limiter{{
				Type: enumsspb.LIMITER_TYPE_CONCURRENCY, Source: LimiterSourceConfig, Key: "key",
			}}}
			tx := r.NewTx(namespace.ID("ns"), task, &testCallback{})
			defer tx.Close()
			lim := tx.limiters[0].limiterTx.(*concurrencyTx).cs
			require.EqualValues(t, 1, lim.tokens)
			require.NoError(t, tx.Check())
			require.Zero(t, batches)
			err := tx.Reserve(context.Background())
			if allow {
				require.NoError(t, err)
				require.NoError(t, tx.Commit(context.Background()))
				require.Equal(t, 2, batches)
			} else {
				require.Error(t, err)
				require.Equal(t, 1, batches)
				require.Zero(t, lim.tokens)
			}
			tx.Close()
		})
	}
}

func TestSkipCheck_DoesNotConsumeOrReturnTokens(t *testing.T) {
	r := NewReadiness(nil, NewReadinessConfig(dynamicconfig.NewNoopCollection()))
	t.Cleanup(r.Stop)
	setTokens(r, 1, 5)

	tx := r.NewTx(testNsID, newConcurrencyTestTask(), &testCallback{})
	require.NoError(t, tx.SkipCheck())
	require.Equal(t, int32(5), getTokens(r))

	tx.Rollback(context.Background())
	require.Equal(t, int32(5), getTokens(r))
}

func newConcurrencyTestTask() *testTask {
	return &testTask{limiters: []Limiter{{
		Type:   enumsspb.LIMITER_TYPE_CONCURRENCY,
		Key:    testConKey,
		Source: LimiterSourceConfig,
	}}}
}

func setTokens(r *Readiness, gen int64, tokens int32) {
	cs := r.getConcurrencyLimiter(testNsID, testConKey)
	cs.reportWaitResponse(gen, tokens, notifySync)
}

func getTokens(r *Readiness) int32 {
	rs := r.getShard(testNsID)
	rs.lock.Lock()
	defer rs.lock.Unlock()
	return rs.concurrencyLimiters[testNsID.String()+testConKey].tokens
}
