package action

import (
	"context"
	"sync"

	"github.com/nexus-rpc/sdk-go/nexus"
	nexuspb "go.temporal.io/api/nexus/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

// ResponsePolicy is a programmable Nexus mock handler: a HandlerResponse action installs the
// start result, and it records the first callback URL/token so a CompletionCallback action can
// complete the operation.
type ResponsePolicy struct {
	mu             sync.Mutex
	onStart        nexus.HandlerStartOperationResult[any]
	startErr       error
	block          bool // hold the start attempt (keeps the operation scheduled) until ctx is done
	deferred       bool
	release        chan struct{}
	releaseOnce    sync.Once
	onStartHook    func(context.Context, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)
	handlerLinks   []nexus.Link
	cancelErr      error
	captured       chan callback
	cancelObserved chan struct{}
	namespaceID    string
	factObserver   factObserver
}

type callback struct{ url, token string }

// NewResponsePolicy returns a policy with no configured response yet (an action installs one).
func NewResponsePolicy() *ResponsePolicy {
	return &ResponsePolicy{
		captured:       make(chan callback, 1),
		cancelObserved: make(chan struct{}, 1),
	}
}

// Handler adapts the policy to a nexustest.Handler for env.createRandomExternalNexusServer.
func (p *ResponsePolicy) Handler() nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(hctx context.Context, _, _ string, _ *nexus.LazyValue, opts nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case p.captured <- callback{opts.CallbackURL, opts.CallbackHeader.Get(commonnexus.CallbackTokenHeader)}:
			default: // already captured (a retry); keep the first
			}
			p.mu.Lock()
			r, err, block, deferred, release, hook := p.onStart, p.startErr, p.block, p.deferred, p.release, p.onStartHook
			links := append([]nexus.Link(nil), p.handlerLinks...)
			namespaceID, observer := p.namespaceID, p.factObserver
			p.mu.Unlock()
			callbackID := ""
			if observer != nil {
				header := make(map[string]string, len(opts.CallbackHeader))
				for key, value := range opts.CallbackHeader {
					header[key] = value
				}
				observed := fact.NewNexusCallbackObservation(namespaceID, &nexuspb.StartOperationRequest{
					RequestId:      opts.RequestID,
					Callback:       opts.CallbackURL,
					CallbackHeader: header,
				})
				callbackID = observed.CallbackID
				if err := observer.ObserveFact(hctx, observed); err != nil {
					return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "umpire callback observation failed")
				}
			}
			if hook != nil {
				r, err = hook(hctx, opts)
			} else {
				if len(links) != 0 {
					nexus.AddHandlerLinks(hctx, links...)
				}
				if block {
					<-hctx.Done() // hold the attempt so the operation stays scheduled
					return nil, hctx.Err()
				}
				if deferred {
					select {
					case <-release:
					case <-hctx.Done():
						return nil, hctx.Err()
					}
				}
			}
			if observer != nil && err == nil {
				if observed := fact.NewNexusHTTPStartResponse(namespaceID, callbackID, opts.RequestID, nexusStartResponse(r)); observed != nil {
					if err := observer.ObserveFact(hctx, observed); err != nil {
						return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "umpire start response observation failed")
					}
				}
			}
			return r, err
		},
		OnCancelOperation: func(_ context.Context, _, _, _ string, _ nexus.CancelOperationOptions) error {
			select {
			case p.cancelObserved <- struct{}{}:
			default:
			}
			p.mu.Lock()
			err := p.cancelErr
			p.cancelErr = nil
			p.mu.Unlock()
			return err
		},
	}
}

func nexusStartResponse(result nexus.HandlerStartOperationResult[any]) *nexuspb.StartOperationResponse {
	switch result := result.(type) {
	case *nexus.HandlerStartOperationResultAsync:
		return &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
			AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: result.OperationToken},
		}}
	case *nexus.HandlerStartOperationResultSync[any]:
		encoded, err := payload.Encode(result.Value)
		if err != nil {
			return nil
		}
		return &nexuspb.StartOperationResponse{Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{Payload: encoded},
		}}
	default:
		return nil
	}
}

func (p *ResponsePolicy) setFactObserver(namespaceID string, observer factObserver) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.namespaceID = namespaceID
	p.factObserver = observer
}

func (p *ResponsePolicy) setStart(r nexus.HandlerStartOperationResult[any], err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStart, p.startErr = r, err
}

func (p *ResponsePolicy) setHandlerLinks(links ...nexus.Link) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handlerLinks = append([]nexus.Link(nil), links...)
}

func (p *ResponsePolicy) setStartHook(hook func(context.Context, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStartHook = hook
}

func (p *ResponsePolicy) setNextCancelError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancelErr = err
}

func (p *ResponsePolicy) setBlock() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.block = true
}

func (p *ResponsePolicy) setDeferredStart(r nexus.HandlerStartOperationResult[any], err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onStart, p.startErr = r, err
	p.deferred = true
	p.release = make(chan struct{})
}

func (p *ResponsePolicy) releaseDeferredStart() {
	p.mu.Lock()
	release := p.release
	p.mu.Unlock()
	if release != nil {
		p.releaseOnce.Do(func() { close(release) })
	}
}
