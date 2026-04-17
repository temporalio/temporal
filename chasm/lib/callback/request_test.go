package callback

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/mock/gomock"
)

func newTestFrontendHTTPClient(ts *httptest.Server) *common.FrontendHTTPClient {
	u, _ := url.Parse(ts.URL)
	return &common.FrontendHTTPClient{
		Client:  *ts.Client(),
		Address: u.Host,
		Scheme:  u.Scheme,
	}
}

func TestRouteRequest_ExternalTarget(t *testing.T) {
	// When no source header is set, the request should be sent via the default client.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)

	r, err := http.NewRequest(http.MethodPost, ts.URL+"/some/path", nil)
	require.NoError(t, err)

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil, // namespaceRegistry not needed for external targets
		nil, // httpClientCache not needed for external targets
		nil, // callbackTokenGenerator not needed for external targets
		ts.Client(),
		nil, // localClient not needed for external targets
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRouteRequest_SourceHeaderLocal(t *testing.T) {
	// When the source header matches the local cluster, the request should be routed to the local client.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)
	clusterMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"cluster-A": {ClusterID: "cluster-id-A"},
	})
	clusterMeta.EXPECT().GetCurrentClusterName().Return("cluster-A")

	localClient := newTestFrontendHTTPClient(ts)

	r, err := http.NewRequest(http.MethodPost, "http://original-host/some/path", nil)
	require.NoError(t, err)
	r.Header.Set(callbackSourceHeader, "cluster-id-A")

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil, // namespaceRegistry
		nil, // httpClientCache - not used since it's the local cluster
		nil, // callbackTokenGenerator
		&http.Client{},
		localClient,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRouteRequest_SourceHeaderUnknownCluster(t *testing.T) {
	// When the source header doesn't match any known cluster, falls back to local client.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)
	clusterMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"cluster-A": {ClusterID: "cluster-id-A"},
	})
	clusterMeta.EXPECT().GetCurrentClusterName().Return("cluster-A")

	localClient := newTestFrontendHTTPClient(ts)

	r, err := http.NewRequest(http.MethodPost, "http://original-host/some/path", nil)
	require.NoError(t, err)
	r.Header.Set(callbackSourceHeader, "unknown-cluster-id")

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil,
		nil,
		nil,
		&http.Client{},
		localClient,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRouteSystemCallbackRequest_NilHeaders(t *testing.T) {
	// When the request has nil headers, it should fall back to the local client.
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	localClient := newTestFrontendHTTPClient(ts)

	r := &http.Request{
		Method: http.MethodPost,
		URL:    &url.URL{Path: "/"},
		Header: nil,
	}

	resp, err := routeSystemCallbackRequest(
		r,
		nil, // clusterMetadata - not needed for nil headers path
		nil, // namespaceRegistry
		nil, // httpClientCache
		nil, // callbackTokenGenerator
		localClient,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, commonnexus.PathCompletionCallbackNoIdentifier, gotPath)
}

func TestRouteSystemCallbackRequest_InvalidToken(t *testing.T) {
	r, err := http.NewRequest(http.MethodPost, commonnexus.SystemCallbackURL, nil)
	require.NoError(t, err)
	r.Header.Set(commonnexus.CallbackTokenHeader, "not-valid-json")

	_, err = routeSystemCallbackRequest(
		r,
		nil,
		nil,
		nil,
		nil,
		nil,
		log.NewNoopLogger(),
	)
	require.Error(t, err)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
	require.Contains(t, handlerErr.Error(), "invalid callback token")
}

func TestRouteSystemCallbackRequest_InvalidTokenData(t *testing.T) {
	// Valid token structure but invalid data field.
	r, err := http.NewRequest(http.MethodPost, commonnexus.SystemCallbackURL, nil)
	require.NoError(t, err)
	r.Header.Set(commonnexus.CallbackTokenHeader, `{"v":1,"d":"!!!invalid-base64"}`)

	tokenGen := commonnexus.NewCallbackTokenGenerator()

	_, err = routeSystemCallbackRequest(
		r,
		nil,
		nil,
		nil,
		tokenGen,
		nil,
		log.NewNoopLogger(),
	)
	require.Error(t, err)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
}

func TestRouteSystemCallbackRequest_NamespaceNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)

	tokenGen := commonnexus.NewCallbackTokenGenerator()
	tokenStr, err := tokenGen.Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "ns-id-1",
		WorkflowId:  "wf-1",
		RunId:       "run-1",
		Ref:         &persistencespb.StateMachineRef{},
	})
	require.NoError(t, err)

	nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id-1")).Return(
		nil, serviceerror.NewNamespaceNotFound("ns-id-1"),
	)

	r, err := http.NewRequest(http.MethodPost, commonnexus.SystemCallbackURL, nil)
	require.NoError(t, err)
	r.Header.Set(commonnexus.CallbackTokenHeader, tokenStr)

	_, err = routeSystemCallbackRequest(
		r,
		nil,
		nsRegistry,
		nil,
		tokenGen,
		nil,
		log.NewNoopLogger(),
	)
	require.Error(t, err)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerErr.Type)
}

func TestRouteSystemCallbackRequest_InvalidChasmComponentRef(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  *persistencespb.ChasmComponentRef
	}{
		{
			name: "missing namespace id",
			ref: &persistencespb.ChasmComponentRef{
				BusinessId: "wf-1",
				RunId:      "run-1",
			},
		},
		{
			name: "missing business id",
			ref: &persistencespb.ChasmComponentRef{
				NamespaceId: "ns-id-1",
				RunId:       "run-1",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tokenGen := commonnexus.NewCallbackTokenGenerator()

			ref, err := tc.ref.Marshal()
			require.NoError(t, err)

			tokenStr, err := tokenGen.Tokenize(&tokenspb.NexusOperationCompletion{ComponentRef: ref})
			require.NoError(t, err)

			r, err := http.NewRequest(http.MethodPost, commonnexus.SystemCallbackURL, nil)
			require.NoError(t, err)
			r.Header.Set(commonnexus.CallbackTokenHeader, tokenStr)

			_, err = routeSystemCallbackRequest(
				r,
				nil,
				nil,
				nil,
				tokenGen,
				nil,
				log.NewNoopLogger(),
			)
			require.Error(t, err)
			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
			require.Contains(t, handlerErr.Error(), "invalid callback token")
		})
	}
}

func TestRouteSystemCallbackRequest_Success(t *testing.T) {
	for _, tc := range []struct {
		name            string
		completionToken func(*commonnexus.CallbackTokenGenerator) (string, error)
	}{
		{
			name: "HSM",
			completionToken: func(tokenGen *commonnexus.CallbackTokenGenerator) (string, error) {
				return tokenGen.Tokenize(&tokenspb.NexusOperationCompletion{
					// HSM sets the deprecated execution fields and ref.
					NamespaceId: "ns-id-1",
					WorkflowId:  "wf-1",
					RunId:       "run-1",
					Ref:         &persistencespb.StateMachineRef{},
				})
			},
		},
		{
			name: "CHASM",
			completionToken: func(tokenGen *commonnexus.CallbackTokenGenerator) (string, error) {
				ref, err := (&persistencespb.ChasmComponentRef{
					NamespaceId: "ns-id-1",
					BusinessId:  "wf-1",
					RunId:       "run-1",
				}).Marshal()
				if err != nil {
					return "", err
				}
				return tokenGen.Tokenize(&tokenspb.NexusOperationCompletion{
					// CHASM sets ComponentRef.
					ComponentRef: ref,
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			ctrl := gomock.NewController(t)
			clusterMeta := cluster.NewMockMetadata(ctrl)
			nsRegistry := namespace.NewMockRegistry(ctrl)

			tokenGen := commonnexus.NewCallbackTokenGenerator()
			tokenStr, err := tc.completionToken(tokenGen)
			require.NoError(t, err)

			testNS := namespace.NewLocalNamespaceForTest(
				&persistencespb.NamespaceInfo{Id: "ns-id-1", Name: "test-ns"},
				nil,
				"cluster-A",
			)
			nsRegistry.EXPECT().GetNamespaceByID(namespace.ID("ns-id-1")).Return(testNS, nil)

			// httpClientCache.Get will fail for "cluster-A", so it falls back to localClient.
			clusterMeta.EXPECT().GetCurrentClusterName().Return("cluster-A").AnyTimes()
			clusterMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{}).AnyTimes()
			clusterMeta.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any())

			localClient := newTestFrontendHTTPClient(ts)
			// Create a cache that will fail for the requested cluster since we don't set up metadata fully.
			httpClientCache := cluster.NewFrontendHTTPClientCache(clusterMeta, nil)

			r, err := http.NewRequest(http.MethodPost, commonnexus.SystemCallbackURL, nil)
			require.NoError(t, err)
			r.Header.Set(commonnexus.CallbackTokenHeader, tokenStr)

			resp, err := routeSystemCallbackRequest(
				r,
				clusterMeta,
				nsRegistry,
				httpClientCache,
				tokenGen,
				localClient,
				log.NewNoopLogger(),
			)
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, commonnexus.PathCompletionCallbackNoIdentifier, gotPath)
		})
	}
}

func TestRouteRequest_SystemCallback(t *testing.T) {
	// Verify that routeRequest delegates to routeSystemCallbackRequest for system callback URLs.
	var gotPath string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	localClient := newTestFrontendHTTPClient(ts)

	// Use nil headers to take the simplest path through routeSystemCallbackRequest.
	r := &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Scheme: "temporal",
			Host:   "system",
		},
		Header: nil,
	}

	resp, err := routeRequest(
		r,
		nil,
		nil,
		nil,
		nil,
		&http.Client{},
		localClient,
		log.NewNoopLogger(),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, commonnexus.PathCompletionCallbackNoIdentifier, gotPath)
}
