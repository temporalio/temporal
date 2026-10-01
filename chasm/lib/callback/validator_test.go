package callback

import (
	"context"
	"encoding/base64"
	"regexp"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/mock/gomock"
)

func TestValidateCallbacks(t *testing.T) {
	allowAll := AddressMatchRules{
		Rules: []AddressMatchRule{
			{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
		},
	}
	v := NewValidator(
		func(string) int { return 10 },
		func(string) int { return 1000 },
		func(string) int { return 4096 },
		func(string) AddressMatchRules { return allowAll },
		nil,
	)

	t.Run("ValidNexusCallback", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"Content-Type": "application/json"},
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		require.NoError(t, err)
	})

	t.Run("TooManyCallbacks", func(t *testing.T) {
		v := NewValidator(
			func(string) int { return 1 },
			func(string) int { return 1000 },
			func(string) int { return 4096 },
			func(string) AddressMatchRules { return allowAll },
			nil,
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb1"}}},
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb2"}}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "cannot attach more than 1 callbacks")
	})

	t.Run("URLTooLong", func(t *testing.T) {
		v := NewValidator(
			func(string) int { return 10 },
			func(string) int { return 50 },
			func(string) int { return 4096 },
			func(string) AddressMatchRules { return allowAll },
			nil,
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost/" + string(make([]byte, 51)),
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "url length longer than max length allowed")
	})

	t.Run("HeaderTooLarge", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"X-Large": string(make([]byte, 5000))},
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "header size longer than max allowed size")
	})

	t.Run("HeaderKeysNormalizedToLowercase", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    "http://localhost:8080/callback",
					Header: map[string]string{"Content-Type": "application/json", "X-Custom": "value"},
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		require.NoError(t, err)
		nexus := cbs[0].GetNexus()
		require.Equal(t, "application/json", nexus.Header["content-type"])
		require.Equal(t, "value", nexus.Header["x-custom"])
		_, hasMixed := nexus.Header["Content-Type"]
		require.False(t, hasMixed)
	})

	t.Run("URLNotInAllowlist", func(t *testing.T) {
		v := NewValidator(
			func(string) int { return 10 },
			func(string) int { return 1000 },
			func(string) int { return 4096 },
			func(string) AddressMatchRules { return AddressMatchRules{} },
			nil,
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost:8080/callback",
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "does not match any configured callback address")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: nil},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "unknown callback variant")
	})

	t.Run("EmptyCallbacksNoError", func(t *testing.T) {
		err := v.Validate(context.Background(), "ns", nil)
		require.NoError(t, err)
	})

	t.Run("InternalCallbackSkipped", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs)
		require.NoError(t, err)
	})
}

func TestValidateInternalCallbackNamespace(t *testing.T) {
	nsName := "ns"
	nsID := namespace.ID(uuid.NewString())
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	registry.EXPECT().GetNamespaceID(namespace.Name(nsName)).Return(nsID, nil).AnyTimes()
	v := NewValidator(
		func(string) int { return 10 },
		func(string) int { return 1000 },
		func(string) int { return 4096 },
		func(string) AddressMatchRules { return AddressMatchRules{} },
		registry,
	)

	refBytes := func(nsID, businessID string) []byte {
		b, err := (&persistencespb.ChasmComponentRef{NamespaceId: nsID, BusinessId: businessID}).Marshal()
		require.NoError(t, err)
		return b
	}
	legacyToken := func(nsID, businessID string) string {
		return base64.RawURLEncoding.EncodeToString(refBytes(nsID, businessID))
	}
	envelopeToken := func(nsID, businessID string) string {
		b, err := (&tokenspb.NexusOperationCompletion{ComponentRef: refBytes(nsID, businessID), RequestId: "req"}).Marshal()
		require.NoError(t, err)
		return base64.RawURLEncoding.EncodeToString(b)
	}

	testCases := []struct {
		name    string
		url     string
		headers map[string]string
		errMsg  string
	}{
		{
			name:    "same namespace legacy token",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: legacyToken(nsID.String(), "sched")},
		},
		{
			name:    "same namespace envelope token",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: envelopeToken(nsID.String(), "sched")},
		},
		{
			name:    "different namespace legacy token",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: legacyToken(uuid.NewString(), "sched")},
			errMsg:  "internal callback must target the same namespace",
		},
		{
			name:    "different namespace envelope token",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: envelopeToken(uuid.NewString(), "sched")},
			errMsg:  "internal callback must target the same namespace",
		},
		{
			name:   "missing token",
			url:    chasm.NexusCompletionHandlerURL,
			errMsg: "missing internal callback token",
		},
		{
			name:    "invalid token encoding",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: "!!!"},
			errMsg:  "invalid internal callback token",
		},
		{
			name:    "missing business ID",
			url:     chasm.NexusCompletionHandlerURL,
			headers: map[string]string{commonnexus.CallbackTokenHeader: legacyToken(nsID.String(), "")},
			errMsg:  "internal callback component reference requires namespace and business IDs",
		},
		{
			name: "system callback is not checked",
			url:  "temporal://system",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cbs := []*commonpb.Callback{{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: tc.url, Header: tc.headers},
				},
			}}
			err := v.Validate(context.Background(), nsName, cbs)
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}
