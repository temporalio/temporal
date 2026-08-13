package callback

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

// allowAllKindsOpts is a stock ValidateOptions for tests.
var allowAllKindsOpts = ValidateOptions{
	EnabledKinds: EnabledCallbackKinds{CallbackKindNexus, CallbackKindWorker},
}

// newTestValidatorConfig returns a config with permissive limits, for tests that tighten only the
// one limit they are about.
func newTestValidatorConfig() ValidatorConfig {
	allowAll := AddressMatchRules{
		Rules: []AddressMatchRule{
			{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
		},
	}

	return ValidatorConfig{
		MaxPerExecution:            func(string) int { return 10 },
		URLMaxLength:               func(string) int { return 1000 },
		HeaderMaxSize:              func(string) int { return 4096 },
		EndpointRules:              func(string) AddressMatchRules { return allowAll },
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
				SourceContext: &commonpb.Payload{Data: []byte("source-context")},
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(), "does not match any configured callback address")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: nil},
		}
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "unknown callback variant")
	})

	t.Run("ValidWorkerCallback", func(t *testing.T) {
		cb := testWorkerCallback()
		cb.GetWorker().SourceContext = &commonpb.Payload{Data: []byte("source-context")}
		require.NoError(t, v.Validate(context.Background(), "ns", []*commonpb.Callback{cb}, allowAllKindsOpts))
	})

	t.Run("EmptyCallbacksNoError", func(t *testing.T) {
		err := v.Validate(context.Background(), "ns", nil, allowAllKindsOpts)
		require.NoError(t, err)
	})

	t.Run("InternalCallbackSkipped", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
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
			err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestValidateEnabledKinds(t *testing.T) {
	ctx := context.Background()
	v := NewValidator(newTestValidatorConfig())
	nexusCb := &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{
		Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"},
	}}
	workerCb := testWorkerCallback()
	internalCb := &commonpb.Callback{Variant: &commonpb.Callback_Internal_{
		Internal: &commonpb.Callback_Internal{Data: []byte("data")},
	}}

	t.Run("AllSupported", func(t *testing.T) {
		require.NoError(t, v.Validate(ctx, "ns",
			[]*commonpb.Callback{nexusCb, workerCb, internalCb},
			allowAllKindsOpts,
		))
	})

	t.Run("NoCallbacks", func(t *testing.T) {
		require.NoError(t, v.Validate(ctx, "ns", nil, ValidateOptions{
			EnabledKinds: EnabledCallbackKinds{CallbackKindNexus},
		}))
	})

	t.Run("NoKindsEnabled", func(t *testing.T) {
		// The zero value supports no client-supplied kinds at all.
		err := v.Validate(ctx, "ns", []*commonpb.Callback{nexusCb}, ValidateOptions{})
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "completion_callbacks[0]: nexus callbacks are not enabled")
	})

	t.Run("InternalAlwaysAllowed", func(t *testing.T) {
		require.NoError(t, v.Validate(ctx, "ns", []*commonpb.Callback{internalCb}, ValidateOptions{}))
	})

	t.Run("DisabledKind", func(t *testing.T) {
		err := v.Validate(ctx, "ns",
			[]*commonpb.Callback{nexusCb, workerCb},
			ValidateOptions{EnabledKinds: EnabledCallbackKinds{CallbackKindNexus}},
		)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(),
			"completion_callbacks[1]: worker callbacks are not enabled for this execution type")
	})

	t.Run("CheckEnabledBeforeValidation", func(t *testing.T) {
		// Confirm we do the "callback kind enabled" check before regular validation.
		invalidCb := testWorkerCallback()
		invalidCb.GetWorker().TaskQueueName = ""
		err := v.Validate(ctx, "ns", []*commonpb.Callback{invalidCb}, ValidateOptions{
			EnabledKinds: EnabledCallbackKinds{CallbackKindNexus},
		})
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(),
			"completion_callbacks[0]: worker callbacks are not enabled for this execution type")
	})

	t.Run("UnsetVariant", func(t *testing.T) {
		err := v.Validate(ctx, "ns", []*commonpb.Callback{{}}, ValidateOptions{
			EnabledKinds: EnabledCallbackKinds{CallbackKindNexus},
		})
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "completion_callbacks[0]: unknown callback variant")
	})
}

func TestConvertEnabledKinds(t *testing.T) {
	for _, tc := range []struct {
		name string
		val  any
		want EnabledCallbackKinds
	}{
		{name: "Nexus", val: []string{"nexus"}, want: EnabledCallbackKinds{CallbackKindNexus}},
		{
			name: "Both",
			val:  []string{"nexus", "worker"},
			want: EnabledCallbackKinds{CallbackKindNexus, CallbackKindWorker},
		},
		{
			name: "OrderPreserved",
			val:  []string{"worker", "nexus"},
			want: EnabledCallbackKinds{CallbackKindWorker, CallbackKindNexus},
		},
		{
			name: "NamesNormalized",
			val:  []any{" Nexus", "WORKER "},
			want: EnabledCallbackKinds{CallbackKindNexus, CallbackKindWorker},
		},
		{name: "DuplicatesDropped", val: []string{"nexus", "nexus"}, want: EnabledCallbackKinds{CallbackKindNexus}},
		{
			// An unrecognized name must not take the kinds beside it down with it, so that a config
			// naming a kind this server is too old to know about still works.
			name: "UnknownNamesIgnored",
			val:  []string{"nexus", "carrier-pigeon", "internal"},
			want: EnabledCallbackKinds{CallbackKindNexus},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// A value that enables no kind at all is almost certainly operator error, so it is rejected:
	// the Collection then logs and falls back to the setting's default rather than silently
	// rejecting every callback attached in the namespace.
	for _, tc := range []struct {
		name   string
		val    any
		errMsg string
	}{
		{name: "NotAList", val: 42, errMsg: "source data must be an array or slice"},
		{name: "Empty", val: []string{}, errMsg: "does not name any known callback kind"},
		{name: "Nil", val: nil, errMsg: "does not name any known callback kind"},
		{
			name:   "OnlyUnknownNames",
			val:    []string{"carrier-pigeon"},
			errMsg: "does not name any known callback kind",
		},
		{
			// Internal callbacks are server-generated and cannot be enabled by an operator.
			name:   "OnlyInternal",
			val:    []string{"internal"},
			errMsg: "does not name any known callback kind",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.ErrorContains(t, err, tc.errMsg)
			require.Nil(t, got)
		})
	}
}

// Confirm that a value which enables no kinds falls back to the setting's default rather than
// disabling all callbacks.
func TestEnabledKinds_InvalidConfigFallsBackToDefault(t *testing.T) {
	dc := dynamicconfig.NewCollection(
		dynamicconfig.StaticClient{
			EnabledWorkflowCallbackKinds.Key(): []string{"carrier-pigeon"},
		},
		log.NewNoopLogger(),
	)
	require.Equal(t,
		EnabledCallbackKinds{CallbackKindNexus},
		EnabledWorkflowCallbackKinds.Get(dc)("ns"),
	)
}

func TestEnabledKindsFromDynamicConfig(t *testing.T) {
	// The settings default to Nexus only: Worker callbacks are opt-in per namespace.
	dc := dynamicconfig.NewNoopCollection()
	for name, setting := range map[string]dynamicconfig.NamespaceTypedSetting[EnabledCallbackKinds]{
		"workflow": EnabledWorkflowCallbackKinds,
		"update":   EnabledWorkflowUpdateCallbackKinds,
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, EnabledCallbackKinds{CallbackKindNexus}, setting.Get(dc)("ns"))
		})
	}
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
