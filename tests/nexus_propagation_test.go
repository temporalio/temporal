package tests

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	cnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/principaltoken"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	propIssuer       = "test-issuer"
	propKID          = "test-kid"
	propCallerSA     = "nexus-caller"
	propEndUser      = "end-user-alice"
	propJWKSPath     = "/_temporal/nexus/principal-token/jwks"
	propTestDuration = 60 * time.Second
)

// TestNexusPrincipalPropagation_TwoClusters exercises signed caller-identity
// propagation across the Nexus hop over two real, independent full clusters
// (built on temporal/temporal via testcore — not TestEnv, no xdc replication):
//
//	caller cluster (mint) ── HTTP ──▶ handler cluster (verify) ──▶ fake worker
//
// It runs both verifier trust modes:
//   - signature: the handler verifies the JWS against the caller's public key.
//   - transport: the handler trusts the connection peer (cell-mTLS analogue)
//     and reads the claims without a signature check.
//
// It asserts the propagated token carries BOTH caller identities kept distinct:
// the service caller (the worker that issued the ScheduleNexusOperation command)
// and the end user (the identity that started the root workflow, sourced from the
// persisted RootCallerPrincipal — not carried through the worker). It then
// asserts those identities surface to the handler worker as caller_principals on
// the poll response, and that the handler's JWKS endpoint serves its public key.
//
// The feature is configured the official way (config.Global.NexusPrincipalPropagation
// on TestClusterConfig), exercising the production config mapper.
func TestNexusPrincipalPropagation_TwoClusters(t *testing.T) {
	for _, mode := range []principaltoken.TrustMode{principaltoken.TrustModeSignature, principaltoken.TrustModeTransport} {
		t.Run(string(mode), func(t *testing.T) {
			runPropagation(t, mode)
		})
	}
}

func runPropagation(t *testing.T, mode principaltoken.TrustMode) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	privDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)

	// Both clusters share the signing key (the caller signs; in signature mode
	// the handler trusts that key, in transport mode it trusts the peer).
	global := config.Global{
		NexusPrincipalPropagation: config.NexusPrincipalPropagation{
			Issuer:         propIssuer,
			SigningKeyID:   propKID,
			TrustMode:      string(mode),
			SigningKeyData: string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})),
			TrustedIssuers: []config.NexusTrustedIssuer{
				{Issuer: propIssuer, KeyID: propKID, PublicKeyData: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))},
			},
		},
	}
	dc := map[dynamicconfig.Key]any{
		dynamicconfig.EnablePrincipalPropagation.Key():   true,
		dynamicconfig.RefreshNexusEndpointsMinWait.Key(): time.Millisecond,
		// RootCallerPrincipal lives on the CHASM workflow component, so the
		// handler-started workflow must be CHASM-backed for the attach assertion.
		dynamicconfig.EnableChasm.Key(): true,
	}

	// In transport mode the handler frontend trusts the connection peer. Tests
	// have no cell mTLS, so override the (deny-all) default PeerTrustFunc with a
	// trust-all one on the handler frontend.
	var handlerFx map[primitives.ServiceName][]fx.Option
	if mode == principaltoken.TrustModeTransport {
		handlerFx = map[primitives.ServiceName][]fx.Option{
			primitives.FrontendService: {fx.Decorate(func(principaltoken.PeerTrustFunc) principaltoken.PeerTrustFunc {
				return func(context.Context) bool { return true }
			})},
		}
	}

	caller := startPropagationCluster(t, "caller", global, dc, nil)
	defer func() { _ = caller.TearDownCluster() }()
	handler := startPropagationCluster(t, "handler", global, dc, handlerFx)
	defer func() { _ = handler.TearDownCluster() }()

	ctx, cancel := context.WithTimeout(testcore.NewContext(), propTestDuration)
	defer cancel()

	ns := "nexus-prop-" + uuid.NewString()[:8]
	registerPropagationNamespace(ctx, t, caller, ns)
	registerPropagationNamespace(ctx, t, handler, ns)

	// Two distinct identities so the propagated token can be asserted to keep
	// them separate: the end user starts the root workflow (becomes its
	// RootCallerPrincipal), while the worker that later completes the workflow
	// task and issues the ScheduleNexusOperation command is the service caller.
	// The authorizer attributes by API: StartWorkflowExecution -> end user;
	// everything else (poll/respond task) -> worker. If the caller-side end-user
	// sourcing were broken and fell back to the immediate caller, EndUser would
	// equal the worker and the assertion below would fail.
	endUserPrincipal := &commonpb.Principal{Type: "users", Name: propEndUser}
	workerPrincipal := &commonpb.Principal{Type: "service-accounts", Name: propCallerSA}
	caller.Host().SetOnAuthorize(func(_ context.Context, _ *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		p := workerPrincipal
		if strings.HasSuffix(ct.APIName, "/StartWorkflowExecution") {
			p = endUserPrincipal
		}
		return authorization.Result{Decision: authorization.DecisionAllow, Principal: p}, nil
	})
	handler.Host().SetOnAuthorize(func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error) {
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})

	handlerTaskQueue := "handler-tq-" + uuid.NewString()
	dispatchURL := fmt.Sprintf("http://%s/%s",
		handler.Host().FrontendHTTPAddress(),
		cnexus.RouteDispatchNexusTaskByNamespaceAndTaskQueue.Path(cnexus.NamespaceAndTaskQueue{Namespace: ns, TaskQueue: handlerTaskQueue}),
	)
	endpointName := "ep-" + uuid.NewString()
	_, err = caller.OperatorClient().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: endpointName,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_External_{External: &nexuspb.EndpointTarget_External{Url: dispatchURL}},
			},
		},
	})
	require.NoError(t, err)

	// Mock handler worker. It plays the role the SDK would: it (1) observes the
	// server-attributed caller_info on the poll response, and (2) forwards the
	// received principal token on a StartWorkflowExecution, so we can show the
	// server re-validates it and attaches the original end-user to that workflow
	// (the cross-user-code-boundary leg) — all without changing the SDK.
	tokenCh := make(chan string, 1)
	callerInfoCh := make(chan *nexuspb.NexusCallerInfo, 1)
	startedWFCh := make(chan string, 1)
	go pollNexusTaskOnce(ctx, t, handler.FrontendClient(), ns, handlerTaskQueue,
		func(res *workflowservice.PollNexusTaskQueueResponse) *nexuspb.Response {
			select {
			case tokenCh <- res.GetRequest().GetHeader()[principaltoken.Header]:
				callerInfoCh <- res.GetCallerInfo()
				// Forward the token on a workflow start, as a real SDK handler
				// would; the server re-validates it and seeds RootCallerPrincipal.
				wfID := "handler-wf-" + uuid.NewString()
				fwdCtx := metadata.AppendToOutgoingContext(ctx, principaltoken.Header,
					res.GetRequest().GetHeader()[principaltoken.Header])
				_, startErr := handler.FrontendClient().StartWorkflowExecution(fwdCtx, &workflowservice.StartWorkflowExecutionRequest{
					Namespace:    ns,
					WorkflowId:   wfID,
					WorkflowType: &commonpb.WorkflowType{Name: "handler-wf"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "handler-wf-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					RequestId:    uuid.NewString(),
				})
				assert.NoError(t, startErr)
				startedWFCh <- wfID
			default:
			}
			return &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{
				StartOperation: &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_SyncSuccess{
					SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: res.GetRequest().GetStartOperation().GetPayload()},
				}},
			}}
		})

	callerWF := func(wctx workflow.Context) (string, error) {
		c := workflow.NewNexusClient(endpointName, "test-service")
		var result string
		return result, c.ExecuteOperation(wctx, "test-operation", "input", workflow.NexusOperationOptions{}).Get(wctx, &result)
	}
	sdkClient, err := sdkclient.Dial(sdkclient.Options{HostPort: caller.Host().FrontendGRPCAddress(), Namespace: ns})
	require.NoError(t, err)
	defer sdkClient.Close()
	callerTaskQueue := "caller-tq-" + uuid.NewString()
	w := sdkworker.New(sdkClient, callerTaskQueue, sdkworker.Options{})
	w.RegisterWorkflow(callerWF)
	require.NoError(t, w.Start())
	defer w.Stop()
	_, err = sdkClient.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{TaskQueue: callerTaskQueue}, callerWF)
	require.NoError(t, err)

	var token string
	select {
	case token = <-tokenCh:
	case <-time.After(30 * time.Second):
		t.Fatal("handler did not receive a Nexus task with a principal token")
	}
	require.NotEmpty(t, token, "expected a signed principal token propagated to the handler")

	// Verify the propagated token the same way the handler's configured verifier
	// would, and assert it carries the caller identity.
	verified, err := propagationVerifier(t, mode, key).Verify(ctx, token)
	require.NoError(t, err, "propagated token must verify under %s trust mode", mode)

	// Service caller is the worker that issued the ScheduleNexusOperation command.
	require.NotNil(t, verified.ServiceCaller)
	require.Equal(t, "service-accounts", verified.ServiceCaller.GetType())
	require.Equal(t, propCallerSA, verified.ServiceCaller.GetName())

	// End user is the identity that started the root workflow, sourced from the
	// workflow's persisted RootCallerPrincipal (NOT carried through the worker).
	// It must be present and distinct from the service caller.
	require.NotNil(t, verified.EndUser, "end-user principal must propagate (from RootCallerPrincipal)")
	require.Equal(t, "users", verified.EndUser.GetType())
	require.Equal(t, propEndUser, verified.EndUser.GetName())
	require.NotEqual(t, verified.ServiceCaller.GetName(), verified.EndUser.GetName(),
		"end-user must be distinct from the service caller, not a fallback to the immediate caller")

	// The worker-observable caller_info on the poll response must carry the same
	// verified identities (service caller + originating end user). This is the
	// end-to-end check that the server attributes and surfaces the identity to
	// handler workers (independent of the raw token).
	var callerInfo *nexuspb.NexusCallerInfo
	select {
	case callerInfo = <-callerInfoCh:
	case <-time.After(5 * time.Second):
		t.Fatal("handler poll response did not include caller_info")
	}
	require.NotNil(t, callerInfo, "expected caller_info on the poll response")
	require.Equal(t, "service-accounts", callerInfo.GetService().GetType())
	require.Equal(t, propCallerSA, callerInfo.GetService().GetName())
	require.Equal(t, "users", callerInfo.GetRoot().GetType())
	require.Equal(t, propEndUser, callerInfo.GetRoot().GetName())

	// Cross-boundary leg: the workflow the mock worker started by forwarding the
	// token must inherit the ORIGINAL end-user as its RootCallerPrincipal (the
	// server re-validated the token and attached it), surfaced on describe.
	var startedWF string
	select {
	case startedWF = <-startedWFCh:
	case <-time.After(10 * time.Second):
		t.Fatal("mock worker did not start a handler workflow")
	}
	desc, err := handler.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: startedWF},
	})
	require.NoError(t, err)
	rootCaller := desc.GetWorkflowExecutionInfo().GetRootCallerPrincipal()
	require.NotNil(t, rootCaller, "handler-started workflow must inherit the original end-user")
	require.Equal(t, "users", rootCaller.GetType())
	require.Equal(t, propEndUser, rootCaller.GetName(),
		"the forwarded token's end-user must be re-validated and attached as RootCallerPrincipal")

	// The handler cluster's JWKS endpoint must serve its public verification key.
	assertJWKSServed(t, handler.Host().FrontendHTTPAddress())
}

// propagationVerifier builds the verifier the handler uses for the given mode:
// signature → JWS against the public key; transport → trust-all peer.
func propagationVerifier(t *testing.T, mode principaltoken.TrustMode, key *ecdsa.PrivateKey) principaltoken.Verifier {
	if mode == principaltoken.TrustModeTransport {
		return principaltoken.NewTransportVerifier(func(context.Context) bool { return true })
	}
	return principaltoken.NewJWSVerifier(principaltoken.JWSVerifierOptions{
		Keys: principaltoken.NewStaticKeyProvider(
			map[string]map[string]crypto.PublicKey{propIssuer: {propKID: key.Public()}}, nil),
	})
}

func assertJWKSServed(t *testing.T, frontendHTTPAddr string) {
	resp, err := http.Get("http://" + frontendHTTPAddr + propJWKSPath) //nolint:noctx
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), propKID, "JWKS must advertise the cluster signing key")
	require.Contains(t, string(body), "\"kty\":\"EC\"")
}

// startPropagationCluster brings up a single independent full cluster (built on
// temporal/temporal via testcore), with the official Global config, dynamic
// config, and optional per-service fx options. No replication — the clusters
// only interact over the Nexus HTTP hop.
func startPropagationCluster(
	t *testing.T,
	name string,
	global config.Global,
	dc map[dynamicconfig.Key]any,
	fxOpts map[primitives.ServiceName][]fx.Option,
) *testcore.TestCluster {
	cfg := &testcore.TestClusterConfig{
		ClusterMetadata: cluster.Config{
			EnableGlobalNamespace:    false,
			FailoverVersionIncrement: 10,
			MasterClusterName:        name,
			CurrentClusterName:       name,
			ClusterInformation: map[string]cluster.ClusterInformation{
				name: {Enabled: true, InitialFailoverVersion: 1},
			},
		},
		HistoryConfig:          testcore.HistoryConfig{NumHistoryShards: 1},
		Persistence:            testcore.GetPersistenceTestDefaults(),
		GlobalConfig:           global,
		DynamicConfigOverrides: dc,
		ServiceFxOptions:       fxOpts,
		EnableMetricsCapture:   true,
	}
	cfg.Persistence.DBName += "_" + name
	c, err := testcore.NewTestClusterFactory().NewCluster(t, cfg, log.NewTestLogger())
	require.NoError(t, err)
	return c
}

func registerPropagationNamespace(ctx context.Context, t *testing.T, c *testcore.TestCluster, ns string) {
	_, err := c.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	require.NoError(t, err)
	await.RequireTruef(t, func() bool {
		_, err := c.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
		return err == nil
	}, 15*time.Second, 100*time.Millisecond, "namespace %q did not become available", ns)
}

// pollNexusTaskOnce is a minimal taskpoller "fake worker": it polls a single
// Nexus task and responds, giving the test direct access to the forwarded
// request headers (where the propagated token lands) without an SDK worker.
func pollNexusTaskOnce(
	ctx context.Context,
	t *testing.T,
	frontendClient workflowservice.WorkflowServiceClient,
	ns, taskQueue string,
	handler func(*workflowservice.PollNexusTaskQueueResponse) *nexuspb.Response,
) {
	res, err := frontendClient.PollNexusTaskQueue(ctx, &workflowservice.PollNexusTaskQueueRequest{
		Namespace: ns,
		Identity:  uuid.NewString(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
	})
	if ctx.Err() != nil {
		return
	}
	// assert (not require) inside this goroutine: require's FailNow must only run
	// in the test's own goroutine.
	assert.NoError(t, err)
	resp := handler(res)
	_, err = frontendClient.RespondNexusTaskCompleted(ctx, &workflowservice.RespondNexusTaskCompletedRequest{
		Namespace: ns,
		Identity:  uuid.NewString(),
		TaskToken: res.TaskToken,
		Response:  resp,
	})
	if err != nil && ctx.Err() == nil {
		assert.NoError(t, err)
	}
}
