package callback

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

var allowAllAddresses = AddressMatchRules{
	Rules: []AddressMatchRule{
		{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
	},
}

// newTestValidatorConfig returns a config with permissive limits, for tests that tighten only the
// one limit they are about.
func newTestValidatorConfig() ValidatorConfig {
	return ValidatorConfig{
		MaxPerExecution:            func(string) int { return 10 },
		URLMaxLength:               func(string) int { return 1000 },
		HeaderMaxSize:              func(string) int { return 4096 },
		EndpointRules:              func(string) AddressMatchRules { return allowAllAddresses },
		WorkerNameMaxLength:        func(string) int { return 1000 },
		WorkerSourceContextMaxSize: func(string) int { return 4096 },
	}
}

func testWorkerCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: "completions-task-queue",
				Service:       "HTTPAdapter",
				Operation:     "DeliverAsWebhook",
			},
		},
	}
}

func TestValidateCallbacks(t *testing.T) {
	v := NewValidator(newTestValidatorConfig())

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
		cfg := newTestValidatorConfig()
		cfg.MaxPerExecution = func(string) int { return 1 }
		v := NewValidator(cfg)
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
		cfg := newTestValidatorConfig()
		cfg.URLMaxLength = func(string) int { return 50 }
		v := NewValidator(cfg)
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
		cfg := newTestValidatorConfig()
		cfg.EndpointRules = func(string) AddressMatchRules { return AddressMatchRules{} }
		v := NewValidator(cfg)
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

	// Whether an execution accepts a Worker callback is decided by VerifyOnlySupportedKinds; a well
	// formed one is valid on its own terms.
	t.Run("ValidWorkerCallback", func(t *testing.T) {
		cb := testWorkerCallback()
		cb.GetWorker().SourceContext = &commonpb.Payload{Data: []byte("source-context")}
		require.NoError(t, v.Validate(context.Background(), "ns", []*commonpb.Callback{cb}))
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

func TestValidateWorkerCallback(t *testing.T) {
	cfg := newTestValidatorConfig()
	cfg.WorkerNameMaxLength = func(string) int { return 10 }
	cfg.WorkerSourceContextMaxSize = func(string) int { return 20 }
	v := NewValidator(cfg)

	for _, tc := range []struct {
		name   string
		mutate func(*commonpb.Callback_Worker)
		errMsg string
	}{
		{
			name:   "task_queue_name is required",
			mutate: func(w *commonpb.Callback_Worker) { w.TaskQueueName = "" },
			errMsg: "completion_callbacks[1].worker.task_queue_name is required",
		},
		{
			name:   "task_queue_name length",
			mutate: func(w *commonpb.Callback_Worker) { w.TaskQueueName = strings.Repeat("x", 11) },
			errMsg: "completion_callbacks[1].worker.task_queue_name exceeds length limit. Length=11 Limit=10",
		},
		{
			name:   "service is required",
			mutate: func(w *commonpb.Callback_Worker) { w.Service = "" },
			errMsg: "completion_callbacks[1].worker.service is required",
		},
		{
			name:   "service length",
			mutate: func(w *commonpb.Callback_Worker) { w.Service = strings.Repeat("x", 11) },
			errMsg: "completion_callbacks[1].worker.service exceeds length limit",
		},
		{
			name:   "operation is required",
			mutate: func(w *commonpb.Callback_Worker) { w.Operation = "" },
			errMsg: "completion_callbacks[1].worker.operation is required",
		},
		{
			name:   "operation length",
			mutate: func(w *commonpb.Callback_Worker) { w.Operation = strings.Repeat("x", 11) },
			errMsg: "completion_callbacks[1].worker.operation exceeds length limit",
		},
		{
			name: "source_context size",
			mutate: func(w *commonpb.Callback_Worker) {
				w.SourceContext = &commonpb.Payload{Data: []byte(strings.Repeat("x", 21))}
			},
			errMsg: "completion_callbacks[1].worker.source_context exceeds size limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := &commonpb.Callback{Variant: &commonpb.Callback_Worker_{
				Worker: &commonpb.Callback_Worker{
					TaskQueueName: "task-queue",
					Service:       "service",
					Operation:     "operation",
				},
			}}
			tc.mutate(cb.GetWorker())
			// Prefixed with an unrelated callback to confirm the reported index.
			cbs := []*commonpb.Callback{
				{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}}},
				cb,
			}
			err := v.Validate(context.Background(), "ns", cbs)
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestVerifyOnlySupportedKinds(t *testing.T) {
	v := NewValidator(newTestValidatorConfig())
	nexusCb := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
		Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"},
	}}
	internalCb := &commonpb.Callback{Variant: &commonpb.Callback_Internal_{
		Internal: &commonpb.Callback_Internal{Data: []byte("data")},
	}}

	t.Run("AllSupported", func(t *testing.T) {
		require.NoError(t, v.VerifyOnlySupportedKinds(
			[]*commonpb.Callback{nexusCb, testWorkerCallback(), internalCb},
			CallbackKindNexus, CallbackKindWorker, CallbackKindInternal,
		))
	})

	t.Run("NoCallbacks", func(t *testing.T) {
		require.NoError(t, v.VerifyOnlySupportedKinds(nil, CallbackKindNexus))
	})

	t.Run("NoSupportedKinds", func(t *testing.T) {
		// An execution type that accepts no callbacks at all rejects every one of them.
		err := v.VerifyOnlySupportedKinds([]*commonpb.Callback{nexusCb})
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "completion_callbacks[0]: nexus callbacks are not supported")
	})

	t.Run("UnsupportedKind", func(t *testing.T) {
		err := v.VerifyOnlySupportedKinds(
			[]*commonpb.Callback{nexusCb, testWorkerCallback()},
			CallbackKindNexus, CallbackKindInternal,
		)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(),
			"completion_callbacks[1]: worker callbacks are not supported for this execution type")
	})

	t.Run("UnsetVariant", func(t *testing.T) {
		// An unset variant is not a kind that could ever be supported, so it reports as unknown
		// rather than as unsupported for this execution type.
		err := v.VerifyOnlySupportedKinds([]*commonpb.Callback{{}}, CallbackKindNexus)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "completion_callbacks[0]: unknown callback variant")
	})
}

func TestKindOf(t *testing.T) {
	for _, tc := range []struct {
		callback *commonpb.Callback
		want     CallbackKind
		wantName string
	}{
		{
			callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{}}},
			want:     CallbackKindNexus,
			wantName: "nexus",
		},
		{
			callback: testWorkerCallback(),
			want:     CallbackKindWorker,
			wantName: "worker",
		},
		{
			callback: &commonpb.Callback{Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}}},
			want:     CallbackKindInternal,
			wantName: "internal",
		},
		{
			callback: &commonpb.Callback{},
			want:     CallbackKindUnspecified,
			wantName: "unspecified",
		},
		{
			callback: nil,
			want:     CallbackKindUnspecified,
			wantName: "unspecified",
		},
	} {
		t.Run(tc.wantName, func(t *testing.T) {
			require.Equal(t, tc.want, KindOf(tc.callback))
			require.Equal(t, tc.wantName, tc.want.String())
		})
	}
}
