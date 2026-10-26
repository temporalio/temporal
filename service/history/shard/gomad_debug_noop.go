//go:build !gomad || !wasip1 || !wasm

package shard

func (*ContextImpl) gomadDebugSnapshot() {}
