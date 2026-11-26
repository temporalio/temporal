package failpointgrpc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// forwardedHeader marks a request that a fan-out already forwarded. A fan-out that receives such a
// request only applies it locally, so fan-outs that are each other's peers don't forward in a loop.
// It is only a loop guard, not a credential.
const forwardedHeader = "x-failpoint-forwarded"

// Target is a FailpointService that a fan-out applies requests to: NewLocalClient of the service of
// this process, or the client of a remote one.
type Target struct {
	// Host identifies the target in errors and in the FailpointInfo.hosts of ListFailpoints.
	Host   string
	Client failpointpb.FailpointServiceClient
	// ForwardHeaders makes the fan-out send the headers named by WithForwardedHeaders, such as
	// the caller's credentials, to this target. Set it only for targets that authorize the caller,
	// so the credentials don't reach other services.
	ForwardHeaders bool
	// Err is set when the target could not be set up, such as when it could not be dialed. The
	// fan-out reports it as the failure of this target and still applies the request to the others.
	Err error
}

// defaultDescribeTimeout bounds the lookup of a target's registry; see WithDescribeTimeout.
const defaultDescribeTimeout = 5 * time.Second

// FanOutOption configures a fan-out.
type FanOutOption func(*fanOut)

// WithForwardedHeaders copies the named headers of an incoming request to the requests the
// fan-out forwards to targets with ForwardHeaders set, such as the authorization headers that
// peers check.
func WithForwardedHeaders(names ...string) FanOutOption {
	return func(f *fanOut) {
		f.forwardedHeaders = append(f.forwardedHeaders, names...)
	}
}

// WithDescribeTimeout bounds the lookup of each target's registry that precedes a mutation or
// listing, so a stalled target can't use up the caller's deadline before the healthy targets get
// the request. A target whose lookup times out fails. The fan-out also limits the lookup to half
// of the time left before the caller's deadline. The default is 5s.
func WithDescribeTimeout(timeout time.Duration) FanOutOption {
	return func(f *fanOut) {
		f.describeTimeout = timeout
	}
}

// NewFanOut returns a FailpointService that applies each request to local and to every peer, so
// one call configures the failpoints of many processes.
//
// peers is called on every request, so it can follow membership changes. If it fails, the request
// fails with Unavailable without being applied anywhere. It must not include local.
//
// Services can share a registry, such as the services of one process. The fan-out asks every
// target for its registry with DescribeRegistry and applies each request once per registry, to
// the first target that serves it (local first). Applying a request to a registry twice is not
// harmless: a second InjectBlock releases an execution that the first one blocked, and a second
// Once injection re-arms a failpoint that already fired.
//
// A request that another fan-out forwarded is only applied to local, which makes it safe for
// fan-outs to be each other's peers.
//
// Mutations and listings are applied to all registries concurrently:
//   - Inject*, DisableFailpoint and DisableAllFailpoints succeed only if every target succeeds.
//     Otherwise the error names each failed target, and the targets that succeeded keep the
//     change: a failed mutation can be partially applied.
//   - ListFailpoints returns the failpoints and fallback counters of all registries, with hosts set
//     to the targets that serve the registry. Like the response of a service, it is kept within a
//     size limit, and reports the entries that it leaves out, those of the targets included.
//   - WaitForBlock succeeds as soon as any registry reports that an execution reached the block.
//     It fails only if every target fails, such as with NotFound or FailedPrecondition from the
//     targets where the failpoint is not registered or not configured to block. It waits on
//     every target, also several of one registry, and starts each wait as soon as the target is
//     resolved, without waiting for the others.
func NewFanOut(local Target, peers func(context.Context) ([]Target, error), opts ...FanOutOption) failpointpb.FailpointServiceServer {
	f := &fanOut{local: local, peers: peers, describeTimeout: defaultDescribeTimeout, listLimit: defaultListLimit}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

type fanOut struct {
	failpointpb.UnimplementedFailpointServiceServer
	local            Target
	peers            func(context.Context) ([]Target, error)
	forwardedHeaders []string
	describeTimeout  time.Duration
	// listLimit is the size in bytes that the response of ListFailpoints is kept within.
	listLimit int
}

// registryGroup is the targets that serve one registry.
type registryGroup struct {
	// target is the target that requests for the registry are sent to.
	target Target
	hosts  []string
}

// request is a request that the fan-out forwards.
type request struct {
	// ctx carries the loop guard.
	ctx context.Context
	// forwardedHeaders are the key-value pairs of the headers to forward to targets with
	// ForwardHeaders set.
	forwardedHeaders []string
}

// ctxFor returns the context to call target with.
func (r request) ctxFor(target Target) context.Context {
	if !target.ForwardHeaders || len(r.forwardedHeaders) == 0 {
		return r.ctx
	}
	return metadata.AppendToOutgoingContext(r.ctx, r.forwardedHeaders...)
}

// plan is the targets of one request: one per registry, and the targets that already failed.
type plan struct {
	request
	groups []registryGroup
	failed []targetResult[struct{}]
}

func (p plan) size() int {
	return len(p.groups) + len(p.failed)
}

// plan resolves the targets of the request in ctx and groups them by registry.
func (f *fanOut) plan(ctx context.Context) (plan, error) {
	if forwarded(ctx) {
		return plan{ctx: ctx, groups: []registryGroup{{target: f.local}}}, nil
	}
	peers, err := f.resolvePeers(ctx)
	if err != nil {
		return plan{}, err
	}
	r := f.newRequest(ctx)
	targets := append([]Target{f.local}, peers...)

	timeout := f.describeTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline)/2)
	}
	describeCtx, cancel := context.WithTimeout(r.ctx, timeout)
	defer cancel()
	describe := r
	describe.ctx = describeCtx
	described := callAll(describe, targets, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.DescribeRegistryResponse, error) {
		return s.DescribeRegistry(ctx, &failpointpb.DescribeRegistryRequest{})
	})
	p := plan{request: r}
	groupIndex := map[string]int{}
	for i, r := range described {
		if r.err != nil {
			p.failed = append(p.failed, targetResult[struct{}]{host: r.host, err: r.err})
			continue
		}
		id := r.resp.GetRegistryId()
		if idx, ok := groupIndex[id]; ok {
			p.groups[idx].hosts = append(p.groups[idx].hosts, r.host)
			continue
		}
		groupIndex[id] = len(p.groups)
		p.groups = append(p.groups, registryGroup{target: targets[i], hosts: []string{r.host}})
	}
	return p, nil
}

// forwarded reports whether another fan-out forwarded the request in ctx.
func forwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(forwardedHeader)) > 0
}

func (f *fanOut) resolvePeers(ctx context.Context) ([]Target, error) {
	peers, err := f.peers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to resolve failpoint peers: %v", err)
	}
	return peers, nil
}

// newRequest returns the request to forward for the incoming request in ctx.
func (f *fanOut) newRequest(ctx context.Context) request {
	var headers []string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		for _, name := range f.forwardedHeaders {
			for _, value := range md.Get(name) {
				headers = append(headers, name, value)
			}
		}
	}
	return request{
		ctx:              metadata.AppendToOutgoingContext(ctx, forwardedHeader, "true"),
		forwardedHeaders: headers,
	}
}

type targetResult[T any] struct {
	host string
	resp T
	err  error
}

// callAll calls call on every target concurrently and returns the results in target order. A
// target with Err set fails with it without being called.
func callAll[T any](
	r request,
	targets []Target,
	call func(context.Context, failpointpb.FailpointServiceClient) (T, error),
) []targetResult[T] {
	results := make([]targetResult[T], len(targets))
	var wg sync.WaitGroup
	for i, target := range targets {
		if target.Err != nil {
			results[i] = targetResult[T]{host: target.Host, err: target.Err}
			continue
		}
		wg.Go(func() {
			resp, err := call(r.ctxFor(target), target.Client)
			results[i] = targetResult[T]{host: target.Host, resp: resp, err: err}
		})
	}
	wg.Wait()
	return results
}

// callGroups calls call on the target of every registry of p concurrently.
func callGroups[T any](
	p plan,
	call func(context.Context, failpointpb.FailpointServiceClient) (T, error),
) []targetResult[T] {
	targets := make([]Target, len(p.groups))
	for i, g := range p.groups {
		targets[i] = g.target
	}
	return callAll(p.request, targets, call)
}

// failure is a failed target in an aggregated error.
type failure struct {
	host string
	err  error
}

func failures[T any](results []targetResult[T]) []failure {
	var fs []failure
	for _, r := range results {
		if r.err != nil {
			fs = append(fs, failure{host: r.host, err: r.err})
		}
	}
	return fs
}

// joinErrors returns nil if no target failed, or one status error that names each failed target.
// Its code is the code the failures share, or Unknown if they differ.
func joinErrors(fs []failure, total int) error {
	if len(fs) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(fs))
	code := codes.OK
	for _, f := range fs {
		s, ok := status.FromError(f.err)
		if !ok {
			// Keeps the code of a context error, such as a timed-out lookup.
			s = status.FromContextError(f.err)
		}
		switch code {
		case codes.OK:
			code = s.Code()
		case s.Code():
		default:
			code = codes.Unknown
		}
		msgs = append(msgs, fmt.Sprintf("%s: %s", f.host, s.Message()))
	}
	return status.Errorf(code, "failpoint request failed on %d of %d targets: %s",
		len(fs), total, strings.Join(msgs, "; "))
}

// applyAll applies a request once to every registry and aggregates the errors.
func applyAll[T any](
	ctx context.Context,
	f *fanOut,
	call func(context.Context, failpointpb.FailpointServiceClient) (T, error),
) (T, error) {
	var zero T
	p, err := f.plan(ctx)
	if err != nil {
		return zero, err
	}
	results := callGroups(p, call)
	if err := joinErrors(append(failures(p.failed), failures(results)...), p.size()); err != nil {
		return zero, err
	}
	// The response is empty, so any target's will do. A plan always has the local target.
	return results[0].resp, nil
}

func (f *fanOut) DisableFailpoint(ctx context.Context, req *failpointpb.DisableFailpointRequest) (*failpointpb.DisableFailpointResponse, error) {
	return applyAll(ctx, f, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.DisableFailpointResponse, error) {
		return s.DisableFailpoint(ctx, req)
	})
}

func (f *fanOut) DisableAllFailpoints(ctx context.Context, req *failpointpb.DisableAllFailpointsRequest) (*failpointpb.DisableAllFailpointsResponse, error) {
	return applyAll(ctx, f, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.DisableAllFailpointsResponse, error) {
		return s.DisableAllFailpoints(ctx, req)
	})
}

func (f *fanOut) InjectPanic(ctx context.Context, req *failpointpb.InjectPanicRequest) (*failpointpb.InjectPanicResponse, error) {
	return applyAll(ctx, f, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.InjectPanicResponse, error) {
		return s.InjectPanic(ctx, req)
	})
}

func (f *fanOut) InjectBlock(ctx context.Context, req *failpointpb.InjectBlockRequest) (*failpointpb.InjectBlockResponse, error) {
	return applyAll(ctx, f, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.InjectBlockResponse, error) {
		return s.InjectBlock(ctx, req)
	})
}

func (f *fanOut) InjectReturn(ctx context.Context, req *failpointpb.InjectReturnRequest) (*failpointpb.InjectReturnResponse, error) {
	return applyAll(ctx, f, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.InjectReturnResponse, error) {
		return s.InjectReturn(ctx, req)
	})
}

// DescribeRegistry describes the registry of the local target.
func (f *fanOut) DescribeRegistry(ctx context.Context, req *failpointpb.DescribeRegistryRequest) (*failpointpb.DescribeRegistryResponse, error) {
	return f.local.Client.DescribeRegistry(ctx, req)
}

func (f *fanOut) ListFailpoints(ctx context.Context, req *failpointpb.ListFailpointsRequest) (*failpointpb.ListFailpointsResponse, error) {
	p, err := f.plan(ctx)
	if err != nil {
		return nil, err
	}
	results := callGroups(p, func(ctx context.Context, s failpointpb.FailpointServiceClient) (*failpointpb.ListFailpointsResponse, error) {
		return s.ListFailpoints(ctx, req)
	})
	if err := joinErrors(append(failures(p.failed), failures(results)...), p.size()); err != nil {
		return nil, err
	}
	var failpoints []*failpointpb.FailpointInfo
	var fallbackKeys []*failpointpb.FallbackKeyInfo
	var failpointsOmitted, fallbackKeysOmitted int64
	for i, r := range results {
		failpointsOmitted += r.resp.GetFailpointsOmitted()
		fallbackKeysOmitted += r.resp.GetFallbackKeysOmitted()
		for _, info := range r.resp.GetFailpoints() {
			// A forwarded request leaves hosts empty, so they are set where they are known.
			if len(info.Hosts) == 0 {
				info.Hosts = p.groups[i].hosts
			}
			failpoints = append(failpoints, info)
		}
		// Each registry counts its own executions, so its fallback counters are reported per
		// registry, like its failpoints.
		for _, info := range r.resp.GetFallbackKeys() {
			if len(info.Hosts) == 0 {
				info.Hosts = p.groups[i].hosts
			}
			fallbackKeys = append(fallbackKeys, info)
		}
	}
	resp := &failpointpb.ListFailpointsResponse{
		Failpoints:          failpoints,
		FallbackKeys:        fallbackKeys,
		FailpointsOmitted:   failpointsOmitted,
		FallbackKeysOmitted: fallbackKeysOmitted,
	}
	// The registries together, with their hosts, can exceed what each of them kept within its limit.
	limitList(resp, f.listLimit)
	return resp, nil
}

func (f *fanOut) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	if forwarded(ctx) {
		return f.local.Client.WaitForBlock(ctx, req)
	}
	r := f.newRequest(ctx)
	// Cancel the waits on the other registries once one reports the block.
	waitCtx, cancel := context.WithCancel(r.ctx)
	defer cancel()
	r.ctx = waitCtx

	results := make(chan waitResult)
	// returned stops the waits that finish after the first success from reporting.
	returned := make(chan struct{})
	defer close(returned)
	// report reports result and whether WaitForBlock still waits for results.
	report := func(result waitResult) bool {
		select {
		case results <- result:
			return true
		case <-returned:
			return false
		}
	}
	start := func(target Target) {
		go func() { report(f.waitOn(r, target, req)) }()
	}

	// The local wait starts before the peers are resolved, and each peer's wait starts as soon as
	// the peer is resolved, so neither a slow membership nor a slow peer delays a block that is
	// reported elsewhere.
	start(f.local)
	go func() {
		// The lookup uses waitCtx, so it is cancelled once WaitForBlock returns.
		peers, err := f.resolvePeers(waitCtx)
		// Report the peers before starting their waits, so their results arrive after it.
		if !report(waitResult{host: "peers", err: err, lookup: true, peers: len(peers)}) {
			return
		}
		for _, peer := range peers {
			start(peer)
		}
	}()

	var fs []failure
	for pending := 2; pending > 0; pending-- {
		result := <-results
		switch {
		case result.lookup:
			pending += result.peers
			if result.err == nil {
				continue
			}
		case result.err == nil:
			return &failpointpb.WaitForBlockResponse{}, nil
		default:
		}
		fs = append(fs, failure{host: result.host, err: result.err})
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return nil, joinErrors(fs, len(fs))
}

// waitResult is the result of WaitForBlock on one target.
type waitResult struct {
	host string
	err  error
	// lookup is set for the result of the peer lookup, which reports the number of peers whose
	// waits start after it.
	lookup bool
	peers  int
}

// waitOn waits for the block on target. Waits don't change the failpoints, so unlike mutations
// they are not deduplicated by registry: every target waits, and a failed wait on one target
// can't hide a block that another target serving the same registry reports.
func (f *fanOut) waitOn(r request, target Target, req *failpointpb.WaitForBlockRequest) waitResult {
	if target.Err != nil {
		return waitResult{host: target.Host, err: target.Err}
	}
	_, err := target.Client.WaitForBlock(r.ctxFor(target), req)
	return waitResult{host: target.Host, err: err}
}
