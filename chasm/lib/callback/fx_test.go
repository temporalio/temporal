package callback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queuescommon "go.temporal.io/server/service/history/queues/common"
	"go.uber.org/mock/gomock"
)

func TestHTTPCallerProviderRejectsRedirects(t *testing.T) {
	t.Parallel()

	for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			var targetHits atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				targetHits.Add(1)
			}))
			t.Cleanup(target.Close)
			redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL, status)
			}))
			t.Cleanup(redirector.Close)

			rpcFactory := common.NewMockRPCFactory(ctrl)
			rpcFactory.EXPECT().CreateLocalFrontendHTTPClient().Return(&common.FrontendHTTPClient{}, nil)

			provider, err := httpCallerProviderProvider(
				nil,
				nil,
				rpcFactory,
				nil,
				log.NewNoopLogger(),
				nil,
				nil,
				&Config{InspectSourceHeader: dynamicconfig.GetBoolPropertyFn(false)},
			)
			require.NoError(t, err)

			client := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
				HTTPCaller: provider(queuescommon.NamespaceIDAndDestination{NamespaceID: "namespace-id", Destination: redirector.URL}),
				Serializer: commonnexus.PayloadSerializer,
			})
			err = client.CompleteOperation(context.Background(), redirector.URL, nexusrpc.CompleteOperationOptions{})
			require.ErrorIs(t, err, commonnexus.ErrRedirectNotAllowed)
			require.False(t, isRetryableCallError(err))
			require.Zero(t, targetHits.Load(), "redirect target must not be contacted")
		})
	}
}
