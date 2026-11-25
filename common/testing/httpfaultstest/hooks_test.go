package httpfaultstest_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/httpfaults"
	"go.temporal.io/server/common/testing/failpoints"
	"go.temporal.io/server/common/testing/httpfaultstest"
	"go.temporal.io/testx/failpoint"
)

var scope = httpfaults.Scope{NamespaceID: "namespace-id"}

func newRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://example.com/cb1", nil)
	require.NoError(t, err)
	return req
}

func setFailpoint(source *failpoints.Source, key failpoint.Key, configure func(*failpoint.Failpoint[error])) {
	fp := failpoint.NewFailpoint[error](key)
	configure(fp)
	source.Failpoints().Set(key, fp)
}

func TestWrap_NoFailpoints(t *testing.T) {
	t.Parallel()

	handlerCalled := false
	next := func(*http.Request) (*http.Response, error) {
		handlerCalled = true
		return &http.Response{StatusCode: http.StatusOK}, nil
	}
	wrapped := httpfaults.Wrap(httpfaultstest.NewGenerator(failpoints.NewEnabledSource()), scope, next)

	resp, err := wrapped(newRequest(t))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, handlerCalled)
}

func TestWrap_BeforeHandler(t *testing.T) {
	t.Parallel()

	injectedErr := errors.New("injected")
	tests := []struct {
		name    string
		key     failpoint.Key
		scope   httpfaults.Scope
		wantErr error
	}{
		{name: "scoped to namespace", key: failpoint.HTTPKey("POST /cb1").ForScope("namespace-id"), scope: scope, wantErr: injectedErr},
		{name: "scoped to other namespace", key: failpoint.HTTPKey("POST /cb1").ForScope("other"), scope: scope, wantErr: nil},
		{name: "unscoped", key: failpoint.HTTPKey("POST /cb1"), scope: scope, wantErr: injectedErr},
		{name: "other operation", key: failpoint.HTTPKey("GET /cb1"), scope: scope, wantErr: nil},
		{name: "any operation", key: failpoint.AnyHTTPKey.ForScope("namespace-id"), scope: scope, wantErr: injectedErr},
		{name: "namespace name only", key: failpoint.AnyHTTPKey.ForScope("namespace-id"), scope: httpfaults.Scope{NamespaceName: "namespace-name"}, wantErr: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			source := failpoints.NewEnabledSource()
			setFailpoint(source, tc.key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWith(func(arg any) error {
					req := arg.(*httpfaults.Request)
					require.Equal(t, "/cb1", req.Raw.URL.Path)
					require.Equal(t, tc.scope, req.Scope)
					return injectedErr
				}, failpoint.Always)
			})
			handlerCalls := 0
			next := func(*http.Request) (*http.Response, error) {
				handlerCalls++
				return &http.Response{StatusCode: http.StatusOK}, nil
			}
			wrapped := httpfaults.Wrap(httpfaultstest.NewGenerator(source), tc.scope, next)

			resp, err := wrapped(newRequest(t))

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, resp)
				require.Equal(t, 0, handlerCalls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, 1, handlerCalls)
		})
	}
}

func TestWrap_ResponseOverrideBeforeHandler(t *testing.T) {
	t.Parallel()

	source := failpoints.NewEnabledSource()
	injected := httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")
	setFailpoint(source, failpoint.HTTPKey("POST /cb1"), func(fp *failpoint.Failpoint[error]) {
		fp.Return(httpfaults.ResponseOverride{Response: injected}, failpoint.Once)
	})
	wrapped := httpfaults.Wrap(httpfaultstest.NewGenerator(source), scope, func(*http.Request) (*http.Response, error) {
		require.FailNow(t, "HTTP call should not run")
		return nil, nil
	})

	resp, err := wrapped(newRequest(t))

	require.NoError(t, err)
	require.Same(t, injected, resp)
}

func TestWrap_AfterHandler(t *testing.T) {
	t.Parallel()

	for _, key := range []failpoint.Key{
		failpoint.HTTPResponseKey("POST /cb1").ForScope("namespace-id"),
		failpoint.AnyHTTPResponseKey.ForScope("namespace-id"),
	} {
		t.Run(key.Name(), func(t *testing.T) {
			t.Parallel()
			source := failpoints.NewEnabledSource()
			original := httpfaults.NewResponse(http.StatusOK, "original")
			setFailpoint(source, key, func(fp *failpoint.Failpoint[error]) {
				fp.CallWith(func(arg any) error {
					result := arg.(httpfaults.Result)
					require.Equal(t, "POST /cb1", result.Operation)
					require.Equal(t, scope, result.Request.Scope)
					require.Same(t, original, result.Response)
					require.NoError(t, result.Error)
					return httpfaults.ResponseOverride{Response: httpfaults.NewResponse(http.StatusServiceUnavailable, "injected")}
				}, failpoint.Always)
			})
			wrapper := httpfaults.Wrap(httpfaultstest.NewGenerator(source), scope, func(*http.Request) (*http.Response, error) {
				return original, nil
			})

			resp, err := wrapper(newRequest(t))

			require.NoError(t, err)
			require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
			require.NoError(t, resp.Body.Close())
		})
	}
}
