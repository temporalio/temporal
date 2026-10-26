//go:build gomad

package sqlplugin

// isConnectionErrno reports false under Gomad: the deterministic runtime
// forbids the syscall package, and the simulated network surfaces a lost
// connection through the driver and io errors ConvertError already handles.
func isConnectionErrno(error) bool {
	return false
}
