package tdbg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace/nsreplication"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	namespaceReplicationRepairExecutionSucceeded = "SUCCEEDED"
	namespaceReplicationRepairExecutionFailed    = "FAILED"
)

type namespaceReplicationRepairExecutionRequest struct {
	VerifyRequest namespaceReplicationVerifyRequest
	Observation   *namespaceReplicationObservation
	Plan          *namespaceReplicationRepairPlan
	Reason        string
}

type namespaceReplicationRepairExecutionResult struct {
	Plan              *namespaceReplicationRepairPlan         `json:"plan"`
	Status            string                                  `json:"status"`
	StatusDetail      string                                  `json:"status_detail,omitempty"`
	Source            namespaceReplicationRepairApplyResult   `json:"source"`
	Targets           []namespaceReplicationRepairApplyResult `json:"targets,omitempty"`
	FinalVerification *namespaceReplicationVerificationResult `json:"final_verification,omitempty"`
}

type namespaceReplicationRepairApplyResult struct {
	Cluster  string `json:"cluster"`
	Action   string `json:"action"`
	Outcome  string `json:"outcome,omitempty"`
	Readback bool   `json:"readback_verified"`
	Error    string `json:"error,omitempty"`
}

type namespaceReplicationRepairer struct {
	verifier *namespaceReplicationVerifier
	// client is the source frontend: the source is written directly, and peers
	// are written through its ApplyRemoteClusterNamespaceRepair relay.
	client     adminservice.AdminServiceClient
	rpcTimeout time.Duration
}

var errNamespaceReplicationRepairRelayUnsupported = errors.New(
	"the source frontend does not support ApplyRemoteClusterNamespaceRepair; upgrade it before repairing replicas",
)

// namespaceRepairEndpoint applies an operator-repair mutation to one cluster and
// reads the namespace back by ID.
type namespaceRepairEndpoint interface {
	Apply(ctx context.Context, request *adminservice.ApplyNamespaceMutationRequest) (*adminservice.ApplyNamespaceMutationResponse, error)
	ReadByID(ctx context.Context, id string) (*adminservice.GetNamespaceResponse, error)
}

type sourceRepairEndpoint struct {
	repairer *namespaceReplicationRepairer
}

func (e sourceRepairEndpoint) Apply(
	ctx context.Context,
	request *adminservice.ApplyNamespaceMutationRequest,
) (*adminservice.ApplyNamespaceMutationResponse, error) {
	return namespaceReplicationRPCCall(ctx, e.repairer.rpcTimeout, func(rpcCtx context.Context) (*adminservice.ApplyNamespaceMutationResponse, error) {
		return e.repairer.client.ApplyNamespaceMutation(rpcCtx, request)
	})
}

func (e sourceRepairEndpoint) ReadByID(ctx context.Context, id string) (*adminservice.GetNamespaceResponse, error) {
	return e.repairer.verifier.reader.SourceNamespaceByID(ctx, id)
}

type peerRepairEndpoint struct {
	repairer *namespaceReplicationRepairer
	cluster  string
}

func (e peerRepairEndpoint) Apply(
	ctx context.Context,
	request *adminservice.ApplyNamespaceMutationRequest,
) (*adminservice.ApplyNamespaceMutationResponse, error) {
	response, err := namespaceReplicationRPCCall(ctx, e.repairer.rpcTimeout, func(rpcCtx context.Context) (*adminservice.ApplyRemoteClusterNamespaceRepairResponse, error) {
		return e.repairer.client.ApplyRemoteClusterNamespaceRepair(rpcCtx, &adminservice.ApplyRemoteClusterNamespaceRepairRequest{
			ClusterName: e.cluster,
			Mutation:    request,
		})
	})
	if err != nil {
		if grpcstatus.Code(err) == codes.Unimplemented {
			return nil, fmt.Errorf("%w: %v", errNamespaceReplicationRepairRelayUnsupported, err)
		}
		return nil, err
	}
	if response.GetClusterName() != e.cluster {
		return nil, fmt.Errorf("cluster identity mismatch: expected %q, got %q", e.cluster, response.GetClusterName())
	}
	return response.GetResponse(), nil
}

func (e peerRepairEndpoint) ReadByID(ctx context.Context, id string) (*adminservice.GetNamespaceResponse, error) {
	return e.repairer.verifier.reader.PeerNamespaceByID(ctx, e.cluster, id)
}

func (r *namespaceReplicationRepairer) Execute(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
) (*namespaceReplicationRepairExecutionResult, error) {
	if request.Plan == nil {
		return nil, errors.New("repair execution requires an executable frozen plan")
	}
	result := &namespaceReplicationRepairExecutionResult{
		Plan:   request.Plan,
		Status: namespaceReplicationRepairExecutionFailed,
		Source: namespaceReplicationRepairApplyResult{
			Cluster: request.Plan.SourceCluster,
			Action:  request.Plan.SourceAction,
		},
	}
	if err := validateNamespaceReplicationRepairExecution(request); err != nil {
		return result, err
	}

	frozenSource, err := r.freezeRepairSource(ctx, request)
	if err != nil {
		return result, err
	}

	preparedSource, err := r.prepareRepairSource(ctx, request, frozenSource, &result.Source)
	if err != nil {
		return result, err
	}

	result.Targets = r.applyRepairTargets(ctx, request, preparedSource)

	finalObservation, finalErr := r.verifier.observe(ctx, request.VerifyRequest)
	if finalObservation != nil {
		result.FinalVerification = finalObservation.result
	}

	failures := namespaceReplicationRepairFailures(result, finalObservation, finalErr)
	if len(failures) != 0 {
		result.StatusDetail = errors.Join(failures...).Error()
		return result, errors.New(result.StatusDetail)
	}
	result.Status = namespaceReplicationRepairExecutionSucceeded
	result.StatusDetail = "Current replicas verified; success does not imply former replicas were synchronized."
	return result, nil
}

func validateNamespaceReplicationRepairExecution(request namespaceReplicationRepairExecutionRequest) error {
	if request.Observation == nil || request.Observation.sourceSnapshot == nil ||
		!request.Observation.sourceStable || request.Reason == "" ||
		request.Plan.RepairID == "" ||
		(request.Plan.Status != namespaceReplicationRepairStatusPlanned &&
			request.Plan.Status != namespaceReplicationRepairStatusNoChanges) {
		return errors.New("repair execution requires an executable frozen plan")
	}
	return nil
}

func (r *namespaceReplicationRepairer) freezeRepairSource(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
) (*adminservice.GetNamespaceResponse, error) {
	frozen, err := r.verifier.reader.SourceNamespaceByID(ctx, request.Plan.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("freeze source namespace: %w", err)
	}
	equal, err := namespaceReplicationSnapshotsEqual(
		request.Observation.sourceSnapshot,
		frozen,
		r.verifier.dataKeysToIgnore,
	)
	if err != nil {
		return nil, fmt.Errorf("compare frozen source namespace: %w", err)
	}
	if !equal {
		return nil, errors.New("source namespace changed after repair planning")
	}
	return frozen, nil
}

func (r *namespaceReplicationRepairer) prepareRepairSource(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
	frozen *adminservice.GetNamespaceResponse,
	result *namespaceReplicationRepairApplyResult,
) (*adminservice.GetNamespaceResponse, error) {
	if request.Plan.SourceAction != namespaceReplicationRepairActionPrepareSource {
		result.Outcome = namespaceReplicationRepairActionNone
		result.Readback = true
		return frozen, nil
	}
	task, err := buildNamespaceReplicationRepairTask(
		frozen,
		frozen,
		r.verifier.dataKeysToIgnore,
	)
	if err != nil {
		return nil, err
	}
	task.ConfigVersion = request.Plan.ProposedSourceConfigVersion
	prepared, err := r.applyRepairAndReadback(ctx, sourceRepairEndpoint{repairer: r}, request, task, result)
	if err != nil {
		result.Error = err.Error()
		return nil, fmt.Errorf("prepare source: %w", err)
	}
	return prepared, nil
}

func (r *namespaceReplicationRepairer) applyRepairTargets(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
	preparedSource *adminservice.GetNamespaceResponse,
) []namespaceReplicationRepairApplyResult {
	var results []namespaceReplicationRepairApplyResult
	for _, clusterPlan := range request.Plan.Clusters {
		if clusterPlan.Action != namespaceReplicationRepairActionNone {
			results = append(results, r.applyTargetRepair(ctx, request, preparedSource, clusterPlan))
		}
	}
	slices.SortFunc(results, func(left, right namespaceReplicationRepairApplyResult) int {
		return cmp.Compare(left.Cluster, right.Cluster)
	})
	return results
}

func namespaceReplicationRepairFailures(
	result *namespaceReplicationRepairExecutionResult,
	finalObservation *namespaceReplicationObservation,
	finalErr error,
) []error {
	var failures []error
	for _, target := range result.Targets {
		if target.Error != "" {
			failures = append(failures, fmt.Errorf("target %s: %s", target.Cluster, target.Error))
		}
	}
	if finalErr != nil {
		failures = append(failures, fmt.Errorf("final verification: %w", finalErr))
	} else if finalObservation == nil || finalObservation.result.Status != namespaceReplicationStatusHealthy {
		failures = append(failures, errors.New("final verification is not healthy"))
	}
	return failures
}

func (r *namespaceReplicationRepairer) applyTargetRepair(
	ctx context.Context,
	request namespaceReplicationRepairExecutionRequest,
	preparedSource *adminservice.GetNamespaceResponse,
	clusterPlan namespaceReplicationClusterRepairPlan,
) (result namespaceReplicationRepairApplyResult) {
	result.Cluster = clusterPlan.Cluster
	result.Action = clusterPlan.Action
	target := request.Observation.targetSnapshots[clusterPlan.Cluster]
	freshTarget, err := r.verifier.reader.PeerNamespaceByID(ctx, clusterPlan.Cluster, request.Plan.NamespaceID)
	if clusterPlan.Action == namespaceReplicationRepairActionCreate {
		if !isNamespaceNotFound(err) {
			result.Error = "target appeared or could not be checked after planning; rerun verification"
			return result
		}
		_, nameErr := r.verifier.reader.PeerNamespaceByName(ctx, clusterPlan.Cluster, preparedSource.GetInfo().GetName())
		if !isNamespaceNotFound(nameErr) {
			result.Error = "target namespace name appeared or could not be checked after planning; rerun verification"
			return result
		}
		target = nil
	} else {
		if err != nil {
			result.Error = fmt.Sprintf("refresh target: %v", err)
			return result
		}
		equal, compareErr := namespaceReplicationSnapshotsEqual(target, freshTarget, r.verifier.dataKeysToIgnore)
		if compareErr != nil || !equal {
			result.Error = "target configuration or versions changed after planning; rerun verification"
			return result
		}
		target = freshTarget
	}
	task, err := buildNamespaceReplicationRepairTask(
		preparedSource,
		target,
		r.verifier.dataKeysToIgnore,
	)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	_, err = r.applyRepairAndReadback(ctx, peerRepairEndpoint{repairer: r, cluster: clusterPlan.Cluster}, request, task, &result)
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func (r *namespaceReplicationRepairer) applyRepairAndReadback(
	ctx context.Context,
	endpoint namespaceRepairEndpoint,
	request namespaceReplicationRepairExecutionRequest,
	task *replicationspb.NamespaceTaskAttributes,
	result *namespaceReplicationRepairApplyResult,
) (*adminservice.GetNamespaceResponse, error) {
	outcome, err := r.applyRepairTask(ctx, endpoint, request, task)
	if err != nil {
		return nil, err
	}
	result.Outcome = outcome.String()
	// Receiver retries can report a no-op after a committed write. Accept that
	// response only when readback proves the requested state and exact versions.
	if !acceptableNamespaceRepairOutcome(result.Action, outcome) {
		return nil, fmt.Errorf("unexpected apply outcome %s", outcome)
	}
	actual, err := endpoint.ReadByID(ctx, task.GetId())
	if err != nil {
		return nil, fmt.Errorf("read back namespace: %w", err)
	}
	if err := validateNamespaceRepairReadback(task, actual, r.verifier.dataKeysToIgnore); err != nil {
		return nil, err
	}
	result.Readback = true
	return actual, nil
}

func (r *namespaceReplicationRepairer) applyRepairTask(
	ctx context.Context,
	endpoint namespaceRepairEndpoint,
	request namespaceReplicationRepairExecutionRequest,
	task *replicationspb.NamespaceTaskAttributes,
) (adminservice.ApplyNamespaceMutationResponse_Outcome, error) {
	payload, err := nsreplication.MarshalNamespaceTask(task)
	if err != nil {
		return adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED, err
	}
	response, err := endpoint.Apply(ctx, &adminservice.ApplyNamespaceMutationRequest{
		SourceCluster:        request.Plan.SourceCluster,
		NamespaceTaskPayload: payload,
		Attribution: &adminservice.NamespaceMutationAttribution{
			Origin:   adminservice.NamespaceMutationAttribution_ORIGIN_OPERATOR_REPAIR,
			RepairId: request.Plan.RepairID,
			Reason:   request.Reason,
		},
	})
	if err != nil {
		return adminservice.ApplyNamespaceMutationResponse_OUTCOME_UNSPECIFIED, err
	}
	return response.GetOutcome(), nil
}

func acceptableNamespaceRepairOutcome(
	action string,
	outcome adminservice.ApplyNamespaceMutationResponse_Outcome,
) bool {
	switch action {
	case namespaceReplicationRepairActionCreate:
		return outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
	case namespaceReplicationRepairActionPrepareSource, namespaceReplicationRepairActionApplySource:
		return outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED ||
			outcome == adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
	default:
		return false
	}
}

func validateNamespaceRepairReadback(
	expected *replicationspb.NamespaceTaskAttributes,
	actual *adminservice.GetNamespaceResponse,
	dataKeysToIgnore map[string]struct{},
) error {
	expectedSnapshot := &adminservice.GetNamespaceResponse{
		Info: proto.CloneOf(expected.GetInfo()), Config: expected.GetConfig(), ReplicationConfig: expected.GetReplicationConfig(),
		FailoverHistory: expected.GetFailoverHistory(), RestoreInfo: expected.GetRestoreInfo(), IsGlobalNamespace: true,
	}
	expectedSnapshot.Info.Id = expected.GetId()
	expectedConfigProjection, expectedFailoverProjection, err := namespaceReplicationProjections(expectedSnapshot, dataKeysToIgnore)
	if err != nil {
		return err
	}
	if !actual.GetIsGlobalNamespace() {
		return errors.New("readback namespace is not global")
	}
	actualConfigProjection, actualFailoverProjection, err := namespaceReplicationProjections(actual, dataKeysToIgnore)
	if err != nil {
		return err
	}
	if !proto.Equal(expectedConfigProjection, actualConfigProjection) {
		return errors.New("config readback does not match prepared source")
	}
	if !proto.Equal(expectedFailoverProjection, actualFailoverProjection) {
		return errors.New("failover readback changed during config repair")
	}
	if actual.GetConfigVersion() != expected.GetConfigVersion() {
		return fmt.Errorf("config version readback is %d, expected %d", actual.GetConfigVersion(), expected.GetConfigVersion())
	}
	if actual.GetFailoverVersion() != expected.GetFailoverVersion() {
		return fmt.Errorf("failover version readback is %d, expected %d", actual.GetFailoverVersion(), expected.GetFailoverVersion())
	}
	return nil
}
