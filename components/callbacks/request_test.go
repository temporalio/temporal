package callbacks

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/nexus"
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
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
		nil,
		ts.Client(),
		nil,
		log.NewNoopLogger(),
		true,
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRouteRequest_SourceHeaderIgnored(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)

	r, err := http.NewRequest(http.MethodPost, ts.URL+"/some/path", nil)
	require.NoError(t, err)
	r.Header.Set(callbackSourceHeader, "cluster-id-A")

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil,
		ts.Client(),
		nil,
		log.NewNoopLogger(),
		false,
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRouteRequest_SourceHeaderInspected(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)
	clusterMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"cluster-A": {ClusterID: "cluster-id-A"},
	})
	clusterMeta.EXPECT().GetCurrentClusterName().Return("cluster-A")

	r, err := http.NewRequest(http.MethodPost, "http://original-host/some/path", nil)
	require.NoError(t, err)
	r.Header.Set(callbackSourceHeader, "cluster-id-A")

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil,
		&http.Client{},
		newTestFrontendHTTPClient(ts),
		log.NewNoopLogger(),
		true,
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRouteRequest_SystemCallback(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	ctrl := gomock.NewController(t)
	clusterMeta := cluster.NewMockMetadata(ctrl)
	clusterMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	clusterMeta.EXPECT().GetCurrentClusterName().Return("cluster-A")

	r, err := http.NewRequest(http.MethodPost, nexus.SystemCallbackURL, nil)
	require.NoError(t, err)

	resp, err := routeRequest(
		r,
		clusterMeta,
		nil,
		&http.Client{},
		newTestFrontendHTTPClient(ts),
		log.NewNoopLogger(),
		false,
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, nexus.PathCompletionCallbackNoIdentifier, r.URL.Path)
}
