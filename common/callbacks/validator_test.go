package callbacks

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
)

func newNexusCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: "https://nexus.ex.xxxxx.cluster.tmprl.cloud:7243/Namespaces/ex.xxxxx/nexus/callback",
				Header: map[string]string{
					"Nexus-Operation-State": "succeeded",
					"Content-Type":          "application/json",
				},
			},
		},
	}
}

func newWorkerCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: "wc-queue",
				Service:       "CompletionService",
				Operation:     "DeliverAsWebhook",
				SourceContext: &commonpb.Payload{Data: []byte("data")},
			},
		},
	}
}

func newValidatorConfig() ValidatorConfig {
	allowAllAddresses := AddressMatchRules{
		Rules: []AddressMatchRule{
			{Regexp: regexp.MustCompile(`.*`), AllowInsecure: true},
		},
	}
	return ValidatorConfig{
		MaxCallbacksPerExecution:   func(string) int { return 10 },
		MaxIDLengthLimit:           func() int { return 10 },
		URLMaxLength:               func(string) int { return 1000 },
		HeaderMaxSize:              func(string) int { return 4096 },
		EndpointRules:              func(string) AddressMatchRules { return allowAllAddresses },
		MaxServiceNameLength:       func(string) int { return 40 },
		MaxOperationNameLength:     func(string) int { return 40 },
		WorkerSourceContextMaxSize: func(string) int { return 1000 },
	}
}

func mustNewValidator(t *testing.T, cfg ValidatorConfig) Validator {
	t.Helper()
	v, err := NewValidator(cfg)
	require.NoError(t, err)
	return v
}

func TestValidatorConfigValidate(t *testing.T) {
	cfg := newValidatorConfig()
	cfg.URLMaxLength = nil
	cfg.EndpointRules = nil

	_, err := NewValidator(cfg)
	require.EqualError(t, err, "missing required fields: [URLMaxLength EndpointRules]")
}

func TestValidateCallbacks(t *testing.T) {
	ctx := context.Background()

	opts := ValidatorOptions{
		EnabledKinds: []Kind{KindNexus, KindWorker},
	}
	v := mustNewValidator(t, newValidatorConfig())

	t.Run("EmptyCallbacksNoError", func(t *testing.T) {
		err := v.Validate(ctx, "ns", nil, opts)
		require.NoError(t, err)
	})

	t.Run("ValidNexusCallback", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			newNexusCallback(),
		}

		err := v.Validate(ctx, "ns", cbs, opts)
		require.NoError(t, err)
	})

	t.Run("ValidWorkerCallback", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			newWorkerCallback(),
		}
		require.NoError(t, v.Validate(ctx, "ns", cbs, opts))
	})

	t.Run("InternalCallbacksFail", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Internal_{
					Internal: &commonpb.Callback_Internal{},
				},
			},
		}

		err := v.Validate(ctx, "ns", cbs, opts)
		require.Error(t, err)
		require.ErrorContains(t, err, "unknown callback variant")
	})

	t.Run("TooManyCallbacks", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			newNexusCallback(),
			newNexusCallback(),
		}

		cfg := newValidatorConfig()
		cfg.MaxCallbacksPerExecution = func(string) int { return 1 }
		v := mustNewValidator(t, cfg)

		err := v.Validate(ctx, "ns", cbs, opts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "cannot attach more than 1 callbacks")
	})

	t.Run("URLTooLong", func(t *testing.T) {
		nexusCb := newNexusCallback()
		nexusCb.GetNexus().Url = "http://localhost/" + string(make([]byte, 51))
		cbs := []*commonpb.Callback{
			newNexusCallback(),
			nexusCb,
		}

		cfg := newValidatorConfig()
		cfg.URLMaxLength = func(string) int { return 50 }
		v := mustNewValidator(t, cfg)

		err := v.Validate(ctx, "ns", cbs, opts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.Error(t, err, "invalid url: url length longer than max length allowed of 50")
	})

	t.Run("HeaderTooLarge", func(t *testing.T) {
		nexusCb := newNexusCallback()
		nexusCb.GetNexus().Header = map[string]string{"X-Large": string(make([]byte, 5000))}
		cbs := []*commonpb.Callback{
			nexusCb,
		}

		err := v.Validate(ctx, "ns", cbs, opts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "invalid header: header size longer than max allowed size of 4096")
	})

	t.Run("HeaderKeysNormalizedToLowercase", func(t *testing.T) {
		nexusCb := newNexusCallback()
		nexusCb.GetNexus().Header = map[string]string{
			"Content-Type": "application/json",
			"X-Custom":     "value",
		}
		cbs := []*commonpb.Callback{
			nexusCb,
		}

		err := v.Validate(ctx, "ns", cbs, opts)
		require.NoError(t, err)

		// Mutation is in-place.
		nexus := nexusCb.GetNexus()
		require.Equal(t, "application/json", nexus.Header["content-type"])
		require.Equal(t, "value", nexus.Header["x-custom"])
		_, hasMixed := nexus.Header["Content-Type"]
		require.False(t, hasMixed)
	})

	t.Run("URLNotInAllowlist", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			newNexusCallback(),
		}

		cfg := newValidatorConfig()
		cfg.EndpointRules = func(string) AddressMatchRules {
			// No rules in the allow list.
			return AddressMatchRules{}
		}
		v := mustNewValidator(t, cfg)

		err := v.Validate(ctx, "ns", cbs, opts)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "does not match any configured callback address")
	})

	t.Run("UnsupportedVariant", func(t *testing.T) {
		cbs := []*commonpb.Callback{
			{
				Variant: nil,
			},
		}

		err := v.Validate(ctx, "ns", cbs, opts)
		var unimplementedErr *serviceerror.Unimplemented
		require.ErrorAs(t, err, &unimplementedErr)
		require.Contains(t, err.Error(), "unknown callback variant")
	})
}

func TestValidateWorkerCallback(t *testing.T) {
	ctx := context.Background()

	cfg := newValidatorConfig()
	v := mustNewValidator(t, cfg)
	opts := ValidatorOptions{
		EnabledKinds: []Kind{KindNexus, KindWorker},
	}

	for _, tc := range []struct {
		name   string
		mutate func(*commonpb.Callback_Worker)
		errMsg string
	}{
		{
			name:   "task_queue is not set",
			mutate: func(w *commonpb.Callback_Worker) { w.TaskQueueName = "" },
			errMsg: "taskQueue is not set",
		},
		{
			name:   "task_queue length exceeds limit",
			mutate: func(w *commonpb.Callback_Worker) { w.TaskQueueName = strings.Repeat("x", 11) },
			errMsg: "taskQueue length exceeds limit",
		},
		{
			name:   "task_queue uses reserved prefix",
			mutate: func(w *commonpb.Callback_Worker) { w.TaskQueueName = "/_sys/tq" },
			errMsg: "task queue name cannot start with reserved prefix /_sys/",
		},
		{
			name:   "service is required",
			mutate: func(w *commonpb.Callback_Worker) { w.Service = "" },
			errMsg: "service is required",
		},
		{
			name:   "service length",
			mutate: func(w *commonpb.Callback_Worker) { w.Service = strings.Repeat("x", 41) },
			errMsg: "service exceeds length limit",
		},
		{
			name:   "operation is required",
			mutate: func(w *commonpb.Callback_Worker) { w.Operation = "" },
			errMsg: "operation is required",
		},
		{
			name:   "operation length",
			mutate: func(w *commonpb.Callback_Worker) { w.Operation = strings.Repeat("x", 41) },
			errMsg: "operation exceeds length limit",
		},
		{
			name: "source_context size",
			mutate: func(w *commonpb.Callback_Worker) {
				w.SourceContext = &commonpb.Payload{Data: []byte(strings.Repeat("x", 1001))}
			},
			errMsg: "source_context exceeds size limit",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := newWorkerCallback()
			tc.mutate(cb.GetWorker())

			cbs := []*commonpb.Callback{
				cb,
			}

			err := v.Validate(ctx, "ns", cbs, opts)
			var invalidArgErr *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgErr)
			require.ErrorContains(t, err, tc.errMsg)
		})
	}
}

func TestValidateEnabledKinds(t *testing.T) {
	ctx := context.Background()
	v := mustNewValidator(t, newValidatorConfig())
	nexusCb := newNexusCallback()
	workerCb := newWorkerCallback()

	allowAllKindsOpts := ValidatorOptions{
		EnabledKinds: []Kind{KindNexus, KindWorker},
	}
	nexusOnlyOpts := ValidatorOptions{
		EnabledKinds: []Kind{KindNexus},
	}

	t.Run("NoCallbacks", func(t *testing.T) {
		require.NoError(t, v.Validate(ctx, "ns", nil, nexusOnlyOpts))
	})

	t.Run("AllSupported", func(t *testing.T) {
		require.NoError(t, v.Validate(ctx, "ns",
			[]*commonpb.Callback{nexusCb, workerCb},
			allowAllKindsOpts,
		))
	})

	t.Run("NoKindsEnabled", func(t *testing.T) {
		// The zero value supports no client-supplied kinds at all.
		err := v.Validate(ctx, "ns", []*commonpb.Callback{nexusCb}, ValidatorOptions{})
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "nexus callbacks are not enabled for this execution type")
	})

	t.Run("DisabledKind", func(t *testing.T) {
		err := v.Validate(ctx, "ns",
			[]*commonpb.Callback{nexusCb, workerCb},
			ValidatorOptions{EnabledKinds: []Kind{KindNexus}},
		)
		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "worker callbacks are not enabled for this execution type")
	})

	t.Run("CheckEnabledBeforeValidation", func(t *testing.T) {
		invalidWorkerCb := newWorkerCallback()
		invalidWorkerCb.GetWorker().TaskQueueName = ""
		err := v.Validate(ctx, "ns", []*commonpb.Callback{invalidWorkerCb}, nexusOnlyOpts)

		var invalidArgErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgErr)
		require.ErrorContains(t, err, "completion_callbacks[0]: worker callbacks are not enabled for this execution type")
	})
}
