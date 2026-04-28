package umpire

import (
	"fmt"
	"sort"
	"sync"
)

// RPCRegistry is the catalog of RPCSpecs keyed by gRPC full-method name. It
// is the lookup table the mutation and validation strategies consult at call
// time.
type RPCRegistry struct {
	mu    sync.RWMutex
	specs map[string]RPCSpec
}

func (r *RPCRegistry) Register(spec RPCSpec) struct{} {
	if err := r.register(spec); err != nil {
		panic(err)
	}
	return struct{}{}
}

func (r *RPCRegistry) Lookup(method string) (RPCSpec, bool) {
	if r == nil {
		return RPCSpec{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.specs[method]
	if !ok {
		return RPCSpec{}, false
	}
	return copyRPCSpec(spec), true
}

func (r *RPCRegistry) Specs() []RPCSpec {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	specs := make([]RPCSpec, 0, len(r.specs))
	for _, spec := range r.specs {
		specs = append(specs, copyRPCSpec(spec))
	}
	sort.Slice(specs, func(i, j int) bool {
		return specs[i].Method < specs[j].Method
	})
	return specs
}

func (r *RPCRegistry) HasMutableRequest(method string) bool {
	spec, ok := r.Lookup(method)
	if !ok {
		return false
	}
	return spec.hasMutableRequest()
}

func (r *RPCRegistry) register(spec RPCSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.specs == nil {
		r.specs = make(map[string]RPCSpec)
	}
	if _, ok := r.specs[spec.Method]; ok {
		return fmt.Errorf("umpire: duplicate RPC spec for %s", spec.Method)
	}
	r.specs[spec.Method] = copyRPCSpec(spec)
	return nil
}
