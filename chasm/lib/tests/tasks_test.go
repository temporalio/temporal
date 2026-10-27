package tests_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/tests"
	"go.temporal.io/server/chasm/lib/tests/gen/testspb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/protorequire"
)

var _ chasm.StandbyTaskValidator[*testspb.TestPayloadTTLSideEffectTask] = (*tests.PayloadTTLSideEffectTaskHandler)(nil)

func TestPayloadTTLSideEffectTaskValidateStandby(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		ttl           time.Duration
		removePayload bool
		missingStore  bool
		valid         bool
	}{
		{name: "expiration matches task - retry", ttl: time.Hour, valid: true},
		{name: "expiration before task - retry", ttl: 30 * time.Minute, valid: true},
		{name: "expiration after task - discard", ttl: 2 * time.Hour},
		{name: "payload removed - discard", ttl: time.Hour, removePayload: true},
		{name: "payload has no TTL - discard"},
		{name: "store missing - propagate read error", missingStore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			registry := chasm.NewRegistry(log.NewNoopLogger())
			require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
			require.NoError(t, registry.Register(tests.Library))
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(clock.NewEventTimeSource().Update(now)))
			ctx := chasm.NewEngineContext(t.Context(), engine)
			key := chasm.ExecutionKey{NamespaceID: "test-namespace", BusinessID: "payload-store"}
			if !tc.missingStore {
				result, err := chasm.StartExecution(ctx, key,
					func(ctx chasm.MutableContext, _ any) (*tests.PayloadStore, error) {
						store, err := tests.NewPayloadStore(ctx)
						if err != nil {
							return nil, err
						}
						_, err = store.AddPayload(ctx, tests.AddPayloadRequest{
							PayloadKey: "payload", Payload: payload.EncodeString("value"), TTL: tc.ttl,
						})
						return store, err
					}, nil)
				require.NoError(t, err)
				key = result.ExecutionKey
			}
			ref := chasm.NewComponentRef[*tests.PayloadStore](key)
			if tc.removePayload {
				_, _, err := chasm.UpdateComponent(ctx, ref, (*tests.PayloadStore).RemovePayload, "payload")
				require.NoError(t, err)
			}

			var before tests.DescribePayloadStoreResponse
			if !tc.missingStore {
				var err error
				before, err = chasm.ReadComponent(ctx, ref, (*tests.PayloadStore).Describe, tests.DescribePayloadStoreRequest{})
				require.NoError(t, err)
			}

			handler := &tests.PayloadTTLSideEffectTaskHandler{}
			valid, err := handler.ValidateStandby(ctx, ref, chasm.TaskInvocation{
				TaskAttributes: chasm.TaskAttributes{ScheduledTime: now.Add(time.Hour)}, Attempt: 3,
			}, &testspb.TestPayloadTTLSideEffectTask{PayloadKey: "payload"})
			require.Equal(t, tc.valid, valid)
			if tc.missingStore {
				var notFound *serviceerror.NotFound
				require.ErrorAs(t, err, &notFound)
				return
			}
			require.NoError(t, err)

			after, err := chasm.ReadComponent(ctx, ref, (*tests.PayloadStore).Describe, tests.DescribePayloadStoreRequest{})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, before.State, after.State)
		})
	}
}
