//go:build !gomad || !wasip1 || !wasm

package gomaddebug

func DeclareProbe(string) Probe                   { return Probe{} }
func RegisterDump(string, func(*DumpWriter))      {}
func RegisterInvariant(string, bool, func() bool) {}
func (Probe) Enabled() bool                       { return false }
func (Probe) Emit(...Field)                       {}
