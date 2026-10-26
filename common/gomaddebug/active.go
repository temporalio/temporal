//go:build gomad && wasip1 && wasm

package gomaddebug

import _ "unsafe"

var state registry

func init() {
	state.send = send
	state.onFailure = incompleteRegistry
	runtimeRegisterLibrary(closeRegistry, state.inspect, &state.mask)
}

func incompleteRegistry() { runtimeRegistryClosed(false) }

func closeRegistry() {
	state.close()
	runtimeRegistryClosed(!state.failed)
}

func DeclareProbe(name string) Probe {
	id := state.declare(kindProbe, name, 0, nil, nil)
	runtimeRegister(kindProbe, id, name, 0)
	return Probe{id: uint8(id)}
}

func RegisterDump(name string, dump func(*DumpWriter)) {
	id := state.declare(kindDump, name, 0, dump, nil)
	runtimeRegister(kindDump, id, name, 0)
}

// RegisterInvariant declares whether failure is monotonic over the searched
// domain. Setting monotonic permits conditional bisect; it is not inferred by
// sampling. Checks run after the application has been frozen.
func RegisterInvariant(name string, monotonic bool, check func() bool) {
	var flags uint32
	if monotonic {
		flags = 1
	}
	id := state.declare(kindCheck, name, flags, nil, check)
	runtimeRegister(kindCheck, id, name, flags)
}

func (p Probe) Enabled() bool { return state.mask[p.id>>6]&(uint64(1)<<(p.id&63)) != 0 }

// Emit accepts only typed primitives and existing strings. Evaluate fields
// inside an Enabled guard so disabled probes cannot perturb the execution.
func (p Probe) Emit(fields ...Field) { state.emit(uint32(p.id), fields) }

func send(p []byte) { runtimeEmit(&p[0], uint32(len(p))) }

//go:linkname runtimeRegisterLibrary runtime.gomadDebugRegisterLibrary
func runtimeRegisterLibrary(close func(), inspect func(kind, id uint32), mask *[4]uint64)

//go:linkname runtimeRegister runtime.gomadDebugRegister
func runtimeRegister(kind, id uint32, name string, flags uint32)

//go:linkname runtimeRegistryClosed runtime.gomadDebugRegistryClosed
func runtimeRegistryClosed(ok bool)

//go:linkname runtimeEmit runtime.gomadDebugEmit
//go:noescape
func runtimeEmit(ptr *byte, n uint32)
