package faults

import (
	"context"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/probability"
)

// Generators contains the independent hooks for each direction of a transport.
type Generators[Req, Resp any] struct {
	Inbound  Generator[Req, Resp]
	Outbound Generator[Req, Resp]
}

// NewConfiguredGenerators builds both directions using the same fault mapping and success check.
func NewConfiguredGenerators[Req, Resp any](cfg *config.TransportFaultInjection, outcome func(string) *Outcome[Resp], fallback Generators[Req, Resp], successful func(Resp, error) bool) (Generators[Req, Resp], error) {
	if err := cfg.Validate(); err != nil {
		return Generators[Req, Resp]{}, err
	}
	if cfg == nil {
		return fallback, nil
	}
	return Generators[Req, Resp]{
		Inbound:  newConfiguredGenerator(cfg.Inbound, outcome, fallback.Inbound, successful),
		Outbound: newConfiguredGenerator(cfg.Outbound, outcome, fallback.Outbound, successful),
	}, nil
}

type configuredGenerator[Req, Resp any] struct {
	request    *probability.Sampler[func() *Outcome[Resp]]
	response   *probability.Sampler[func() *Outcome[Resp]]
	fallback   Generator[Req, Resp]
	successful func(Resp, error) bool
}

func newConfiguredGenerator[Req, Resp any](cfg *config.CallFaultInjection, outcome func(string) *Outcome[Resp], fallback Generator[Req, Resp], successful func(Resp, error) bool) Generator[Req, Resp] {
	if cfg == nil {
		return fallback
	}
	enabled := false
	for _, probabilities := range []map[string]float64{cfg.Request.Errors, cfg.Response.Errors} {
		for _, rate := range probabilities {
			enabled = enabled || rate > 0
		}
	}
	if !enabled {
		return fallback
	}
	// Construct outcomes per injection because HTTP response bodies are consumable.
	value := func(name string, _ float64) func() *Outcome[Resp] {
		return func() *Outcome[Resp] { return outcome(name) }
	}
	return &configuredGenerator[Req, Resp]{
		request:    probability.NewSampler(cfg.Request.Errors, cfg.Request.Seed, value),
		response:   probability.NewSampler(cfg.Response.Errors, cfg.Response.Seed, value),
		fallback:   fallback,
		successful: successful,
	}
}

func (g *configuredGenerator[Req, Resp]) GenerateRequest(ctx context.Context, operation string, req Req) *Outcome[Resp] {
	if g.fallback != nil {
		if outcome := g.fallback.GenerateRequest(ctx, operation, req); outcome != nil {
			return outcome
		}
	}
	if outcome, ok := g.request.Sample(); ok {
		return outcome()
	}
	return nil
}

func (g *configuredGenerator[Req, Resp]) GenerateResponse(ctx context.Context, operation string, req Req, resp Resp, err error) *Outcome[Resp] {
	if g.fallback != nil {
		if outcome := g.fallback.GenerateResponse(ctx, operation, req, resp, err); outcome != nil {
			return outcome
		}
	}
	if !g.successful(resp, err) {
		return nil
	}
	if outcome, ok := g.response.Sample(); ok {
		return outcome()
	}
	return nil
}
