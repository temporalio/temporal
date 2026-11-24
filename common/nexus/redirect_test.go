package nexus

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRejectRedirects(t *testing.T) {
	t.Parallel()

	for _, status := range []int{
		http.StatusMultipleChoices,   // 300: never followed by Go.
		http.StatusMovedPermanently,  // 301: Go rewrites POST to GET.
		http.StatusFound,             // 302: Go rewrites POST to GET.
		http.StatusSeeOther,          // 303: Go rewrites POST to GET.
		http.StatusTemporaryRedirect, // 307: Go replays the POST.
		http.StatusPermanentRedirect, // 308: Go replays the POST.
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()

			var targetHits atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				targetHits.Add(1)
			}))
			defer target.Close()

			redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// The POST body has no GetBody, as with the nexusrpc clients, so Go would not replay it on a 307 or 308.
				http.Redirect(w, r, target.URL+"/latest/meta-data/", status)
			}))
			defer redirector.Close()

			client := &http.Client{Transport: RejectRedirects(http.DefaultTransport)}
			resp, err := client.Post(redirector.URL, "application/json", io.NopCloser(strings.NewReader("{}")))
			if resp != nil {
				_ = resp.Body.Close()
			}
			require.ErrorIs(t, err, ErrRedirectNotAllowed)
			require.Zero(t, targetHits.Load(), "redirect target must not be contacted")
		})
	}
}

func TestRejectRedirects_PassesThroughNonRedirects(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := &http.Client{Transport: RejectRedirects(http.DefaultTransport)}
	resp, err := client.Post(server.URL, "application/json", strings.NewReader("{}"))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
