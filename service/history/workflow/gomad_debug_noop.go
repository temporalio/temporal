//go:build !gomad || !wasip1 || !wasm

package workflow

func (*MutableStateImpl) gomadDebugSnapshot(string) {}
