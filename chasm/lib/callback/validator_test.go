package callback

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
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
		err := v.Validate("ns", cbs)
		require.NoError(t, err)
	})

	t.Run("TooManyCallbacks", func(t *testing.T) {
		v := NewValidator(
			func(string) int { return 1 },
			func(string) int { return 1000 },
			func(string) int { return 4096 },
			func(string) AddressMatchRules { return allowAll },
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb1"}}},
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb2"}}},
		}
		err := v.Validate("ns", cbs)
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
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost/" + string(make([]byte, 51)),
				},
			}},
		}
		err := v.Validate("ns", cbs)
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
		err := v.Validate("ns", cbs)
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
		err := v.Validate("ns", cbs)
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
		)
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost:8080/callback",
				},
			}},
		}
		err := v.Validate("ns", cbs)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "does not match any configured callback address")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: nil},
		}
		err := v.Validate("ns", cbs)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "unknown callback variant")
	})

	t.Run("EmptyCallbacksNoError", func(t *testing.T) {
		err := v.Validate("ns", nil)
		require.NoError(t, err)
	})

	t.Run("InternalCallbackSkipped", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			}},
		}
		err := v.Validate("ns", cbs)
		require.NoError(t, err)
	})
}
