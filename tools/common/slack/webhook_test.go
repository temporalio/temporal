package slack

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageSend(t *testing.T) {
	type receivedRequest struct {
		contentType string
		body        []byte
		err         error
	}
	received := make(chan receivedRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		received <- receivedRequest{
			contentType: r.Header.Get("Content-Type"),
			body:        body,
			err:         err,
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	message := NewMessage("fallback")
	message.AddSection("details")
	require.NoError(t, message.Send(server.URL))

	request := <-received
	require.NoError(t, request.err)
	require.Equal(t, "application/json", request.contentType)

	var sent Message
	require.NoError(t, json.Unmarshal(request.body, &sent))
	require.Equal(t, message, &sent)
}

func TestMessageSendRejectsEmptyWebhookURL(t *testing.T) {
	require.EqualError(t, NewMessage("fallback").Send(""), "webhook URL is empty")
}

func TestMessageSendRejectsUnsuccessfulResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	require.EqualError(
		t,
		NewMessage("fallback").Send(server.URL),
		"unexpected Slack response status: 400 Bad Request",
	)
}
