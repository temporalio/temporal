package migration

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Workflow state -> CHASM -> workflow state is the identity for every workflow-representable
// field; pending task-adds travel separately in the request.
func TestRoundTrip(t *testing.T) {
	for seed := range uint64(500) {
		r := rand.New(rand.NewPCG(seed, seed))
		args := randomArgs(r)
		pending := args.State.PendingTaskAddSignals

		req, err := WorkflowStateToCHASM("wci-id", args)
		require.NoError(t, err)
		require.Equal(t, "wci-id", req.GetInstanceId())
		require.Equal(t, args.NamespaceId, req.GetNamespaceId())
		require.Equal(t, wcipb.WCI_STATUS_RUNNING, req.GetState().GetStatus())

		got, err := CHASMStateToWorkflow(args.NamespaceName, args.NamespaceId, req.GetState(), req.GetScalingGroups())
		require.NoError(t, err)

		args.State.PendingTaskAddSignals = nil
		require.Empty(t, cmp.Diff(args, got, protocmp.Transform()), "seed %d", seed)

		require.Len(t, req.GetPendingTaskAdds(), len(pending), "seed %d", seed)
		for i, event := range req.GetPendingTaskAdds() {
			require.Equal(t, pending[i], TaskAddToWorkflow(event), "seed %d", seed)
		}
	}
}

func TestNewGroupsAreIdleWithUnknownSize(t *testing.T) {
	req, err := WorkflowStateToCHASM("wci-id", &iface.WorkerControllerInstanceWorkflowArgs{
		State: &iface.WorkerControllerInstanceLocalState{
			Spec: &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{"g": {}}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, wcipb.SCALING_GROUP_STATUS_IDLE, req.GetScalingGroups()["g"].GetStatus())
	require.EqualValues(t, AppliedSizeUnknown, req.GetScalingGroups()["g"].GetAppliedSize())
}

func TestUnknownValidationResultFails(t *testing.T) {
	_, err := WorkflowStateToCHASM("wci-id", &iface.WorkerControllerInstanceWorkflowArgs{
		State: &iface.WorkerControllerInstanceLocalState{ValidationStatus: &iface.ValidationStatus{Status: "bogus"}},
	})
	require.Error(t, err)
}

func randomArgs(r *rand.Rand) *iface.WorkerControllerInstanceWorkflowArgs {
	local := &iface.WorkerControllerInstanceLocalState{
		CreateTime:           maybeTimestamp(r),
		NextPollTime:         maybeTimestamp(r),
		LastModifierIdentity: randomString(r),
	}
	if r.IntN(2) == 0 {
		local.ConflictToken = []byte(randomString(r) + "x")
	}
	if r.IntN(4) > 0 {
		local.Spec = &iface.WorkerControllerInstanceSpec{ScalingGroupSpecs: map[string]iface.ScalingGroupSpec{}}
		for i := range r.IntN(3) {
			local.Spec.ScalingGroupSpecs[fmt.Sprintf("g%d", i)] = randomSpec(r)
		}
	}
	if n := r.IntN(3); n > 0 {
		local.ScalingStatus = map[string]iface.ScalingAlgorithmStatus{}
		for i := range n {
			local.ScalingStatus[fmt.Sprintf("g%d", i+r.IntN(2))] = randomStatus(r)
		}
	}
	if r.IntN(2) == 0 {
		local.ValidationStatus = &iface.ValidationStatus{
			Status:     []iface.ValidationResult{"", iface.ValidationResultSuccess, iface.ValidationResultFailed}[r.IntN(3)],
			ErrMessage: randomString(r),
		}
		if r.IntN(2) == 0 {
			local.ValidationStatus.LastValidationTime = time.Unix(r.Int64N(1<<32), r.Int64N(1e9)).UTC()
		}
	}
	for range r.IntN(3) {
		local.PendingTaskAddSignals = append(local.PendingTaskAddSignals, &iface.SignalTaskAddRequest{
			TaskQueueName:               randomString(r),
			TaskQueueType:               enumspb.TaskQueueType(r.IntN(4)),
			IsSyncMatch:                 r.IntN(2) == 0,
			SyncMatchSignalsSinceLast:   r.IntN(100),
			NoSyncMatchSignalsSinceLast: r.IntN(100),
		})
	}
	return &iface.WorkerControllerInstanceWorkflowArgs{
		NamespaceName:  "ns-" + randomString(r),
		NamespaceId:    "id-" + randomString(r),
		DeploymentName: "dep-" + randomString(r),
		BuildId:        "build-" + randomString(r),
		State:          local,
	}
}

func randomSpec(r *rand.Rand) iface.ScalingGroupSpec {
	spec := iface.ScalingGroupSpec{
		Compute: iface.ComputeProviderSpec{
			ProviderType:  iface.ComputeProviderType(randomString(r)),
			Config:        maybePayload(r),
			NexusEndpoint: randomString(r),
		},
	}
	for range r.IntN(3) {
		spec.TaskTypes = append(spec.TaskTypes, enumspb.TaskQueueType(1+r.IntN(3)))
	}
	if r.IntN(2) == 0 {
		spec.Scaling = &iface.ScalingAlgorithmSpec{ScalingAlgorithm: iface.ScalingAlgorithmType(randomString(r)), Config: maybePayload(r)}
	}
	return spec
}

// JSON-native values only: the workflow already round-trips status through JSON on every CaN.
func randomStatus(r *rand.Rand) iface.ScalingAlgorithmStatus {
	if r.IntN(5) == 0 {
		return nil
	}
	status := iface.ScalingAlgorithmStatus{}
	for i := range r.IntN(4) {
		switch r.IntN(3) {
		case 0:
			status[fmt.Sprintf("k%d", i)] = float64(r.IntN(1000))
		case 1:
			status[fmt.Sprintf("k%d", i)] = randomString(r)
		default:
			status[fmt.Sprintf("k%d", i)] = r.IntN(2) == 0
		}
	}
	return status
}

func maybePayload(r *rand.Rand) *commonpb.Payload {
	if r.IntN(2) == 0 {
		return nil
	}
	return &commonpb.Payload{Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(randomString(r))}
}

func maybeTimestamp(r *rand.Rand) *timestamppb.Timestamp {
	if r.IntN(2) == 0 {
		return nil
	}
	return timestamppb.New(time.Unix(r.Int64N(1<<32), r.Int64N(1e9)))
}

func randomString(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, r.IntN(6))
	for i := range b {
		b[i] = letters[r.IntN(len(letters))]
	}
	return string(b)
}
