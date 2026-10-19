package faultinjection

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
)

// Config assigns an absolute probability to each fault name.
type Config struct {
	Errors map[string]float64 `yaml:"errors"`
	Seed   int64              `yaml:"seed"`
}

// Generators contains independent fault policies for inbound and outbound calls.
type Generators[Req, Resp any] struct {
	Inbound  Generator[Req, Resp]
	Outbound Generator[Req, Resp]
}

func (c Config) Validate() error {
	total := 0.0
	for _, name := range slices.Sorted(maps.Keys(c.Errors)) {
		probability := c.Errors[name]
		if math.IsNaN(probability) || probability < 0 || probability > 1 {
			return fmt.Errorf("errors.%s: probability must be between 0 and 1", name)
		}
		total += probability
	}
	if total > 1 {
		return errors.New("error probabilities must sum to at most 1")
	}
	return nil
}

// Callbacks adapts programmatic fault selection to a Generator.
type Callbacks[Req, Resp any] struct {
	Request  RequestCallback[Req, Resp]
	Response ResponseCallback[Req, Resp]
}

func (g Callbacks[Req, Resp]) GenerateRequest(ctx context.Context, operation string, req Req) *Outcome[Resp] {
	if g.Request != nil {
		return g.Request(ctx, operation, req)
	}
	return nil
}

func (g Callbacks[Req, Resp]) GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
	if g.Response != nil {
		return g.Response(ctx, operation, req, resp, err)
	}
	return nil
}

// WithTestHooks gives programmatic hooks precedence over configured random faults.
func WithTestHooks[Req, Resp any](configuredGenerator, testHookGenerator Generator[Req, Resp]) Generator[Req, Resp] {
	if testHookGenerator == nil {
		return configuredGenerator
	}
	if configuredGenerator == nil {
		return testHookGenerator
	}
	return Callbacks[Req, Resp]{
		Request: func(ctx context.Context, operation string, req Req) *Outcome[Resp] {
			if outcome := testHookGenerator.GenerateRequest(ctx, operation, req); outcome != nil {
				return outcome
			}
			return configuredGenerator.GenerateRequest(ctx, operation, req)
		},
		Response: func(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
			if outcome := testHookGenerator.GenerateResponse(ctx, operation, req, resp, err); outcome != nil {
				return outcome
			}
			return configuredGenerator.GenerateResponse(ctx, operation, req, resp, err)
		},
	}
}

// NewConfiguredGenerator constructs the shared request/response selection policy.
// Outcomes are constructed per injection because responses may own consumable bodies.
func NewConfiguredGenerator[Req, Resp any](request, response Config, outcome func(string, float64) *Outcome[Resp], successful func(Resp) bool) (Generator[Req, Resp], error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if err := response.Validate(); err != nil {
		return nil, err
	}
	hasFaults := func(probabilities map[string]float64) bool {
		for _, rate := range probabilities {
			if rate > 0 {
				return true
			}
		}
		return false
	}
	if !hasFaults(request.Errors) && !hasFaults(response.Errors) {
		return nil, nil
	}
	requests := newSampler(request)
	responses := newSampler(response)
	sample := func(sampler *sampler) *Outcome[Resp] {
		if selected := sampler.sample(); selected != nil {
			return outcome(selected.name, selected.rate)
		}
		return nil
	}
	return Callbacks[Req, Resp]{
		Request: func(context.Context, string, Req) *Outcome[Resp] { return sample(requests) },
		Response: func(_ context.Context, _ string, _ Req, resp Resp, err error) *Outcome[Resp] {
			if err != nil || (successful != nil && !successful(resp)) {
				return nil
			}
			return sample(responses)
		},
	}, nil
}

// Invoke applies the same fault execution policy to datastore and transport calls.
func Invoke[Req, Resp any](ctx context.Context, generator Generator[Req, Resp], operation string, req Req, call func() (Resp, error)) (Resp, error) {
	if generator == nil {
		return call()
	}
	if outcome := generator.GenerateRequest(ctx, operation, req); outcome != nil {
		if !outcome.AfterSuccess {
			return outcome.Response, outcome.Error
		}
		resp, err := call()
		if err != nil {
			return resp, err
		}
		return resp, outcome.Error
	}
	resp, err := call()
	if outcome := generator.GenerateResponse(ctx, operation, req, resp, err); outcome != nil {
		return outcome.Response, outcome.Error
	}
	return resp, err
}
