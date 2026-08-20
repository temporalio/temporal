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

// allowAllKindsOpts is a stock ValidateOptions for tests.
var allowAllKindsOpts = ValidateOptions{
	EnabledKinds: EnabledCallbackKinds{KindNexus, KindWorker},
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
		// Prefixed with an unrelated callback to confirm the reported index.
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}}},
			{Variant: &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url: "http://localhost/" + string(make([]byte, 51)),
				},
			}},
		}
		err := v.Validate(context.Background(), "ns", cbs, allowAllKindsOpts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Contains(t, err.Error(),
			"completion_callbacks[1]: invalid url: url length longer than max length allowed of 50")
	})

	t.Run("HeaderTooLarge", func(t *testing.T) {
		// Prefixed with an unrelated callback to confirm the reported index.
		cbs := []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: "http://localhost/cb"}}},
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
		require.Contains(t, err.Error(),
			"completion_callbacks[1]: invalid header: header size longer than max allowed size of 4096")
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
			EnabledKinds: EnabledCallbackKinds{KindNexus},
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
			ValidateOptions{EnabledKinds: EnabledCallbackKinds{KindNexus}},
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
			EnabledKinds: EnabledCallbackKinds{KindNexus},
		})
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(),
			"completion_callbacks[0]: worker callbacks are not enabled for this execution type")
	})

	t.Run("UnsetVariant", func(t *testing.T) {
		err := v.Validate(ctx, "ns", []*commonpb.Callback{{}}, ValidateOptions{
			EnabledKinds: EnabledCallbackKinds{KindNexus},
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
		{name: "Nexus", val: []string{"nexus"}, want: EnabledCallbackKinds{KindNexus}},
		{
			name: "Both",
			val:  []string{"nexus", "worker"},
			want: EnabledCallbackKinds{KindNexus, KindWorker},
		},
		{
			name: "OrderPreserved",
			val:  []string{"worker", "nexus"},
			want: EnabledCallbackKinds{KindWorker, KindNexus},
		},
		{
			name: "NamesNormalized",
			val:  []any{" Nexus", "WORKER "},
			want: EnabledCallbackKinds{KindNexus, KindWorker},
		},
		{name: "DuplicatesDropped", val: []string{"nexus", "nexus"}, want: EnabledCallbackKinds{KindNexus}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Any unrecognized name, and a value that names no kind at all, is almost certainly operator
	// error, so the whole value is rejected: the Collection then logs and falls back to the
	// setting's default rather than silently enabling something other than what was configured.
	for _, tc := range []struct {
		name   string
		val    any
		errMsg string
	}{
		{name: "NotAList", val: 42, errMsg: "source data must be an array or slice"},
		{name: "Empty", val: []string{}, errMsg: "no callback kinds named"},
		{name: "Nil", val: nil, errMsg: "no callback kinds named"},
		{
			name:   "OnlyUnknownNames",
			val:    []string{"carrier-pigeon"},
			errMsg: `[carrier-pigeon] does not name a known callback kind`,
		},
		{
			// A typo beside a valid name must not be silently dropped: the operator would otherwise
			// see that kind's callbacks fail with Unimplemented and nothing pointing at the config.
			name:   "UnknownNameBesideKnownName",
			val:    []string{"nexsus", "worker"},
			errMsg: `[nexsus] does not name a known callback kind`,
		},
		{
			// Internal callbacks are server-generated and cannot be enabled by an operator.
			name:   "Internal",
			val:    []string{"nexus", "internal"},
			errMsg: `[internal] does not name a known callback kind`,
		},
		{
			name:   "AllUnknownNamesReported",
			val:    []string{"nexsus", "wroker"},
			errMsg: `[nexsus wroker] does not name a known callback kind`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertEnabledKinds(tc.val)
			require.ErrorContains(t, err, tc.errMsg)
			require.Nil(t, got)
		})
	}
}

func TestKindOf(t *testing.T) {
	for _, tc := range []struct {
		callback *commonpb.Callback
		want     Kind
		wantName string
	}{
		{
			callback: &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{}}},
			want:     KindNexus,
			wantName: "nexus",
		},
		{
			callback: testWorkerCallback(),
			want:     KindWorker,
			wantName: "worker",
		},
		{
			callback: &commonpb.Callback{Variant: &commonpb.Callback_Internal_{Internal: &commonpb.Callback_Internal{}}},
			want:     KindInternal,
			wantName: "internal",
		},
		{
			callback: &commonpb.Callback{},
			want:     KindUnspecified,
			wantName: "unspecified",
		},
		{
			callback: nil,
			want:     KindUnspecified,
			wantName: "unspecified",
		},
	} {
		t.Run(tc.wantName, func(t *testing.T) {
			require.Equal(t, tc.want, KindOf(tc.callback))
			require.Equal(t, tc.wantName, tc.want.String())
		})
	}
}
