//go:build !test_dep

package failpoint

type registryNoop struct{}

// NewRegistry creates a no-op failpoint registry for production builds.
func NewRegistry() Registry {
	return &registryNoop{}
}

func (r *registryNoop) Get(key Key) any {
	return nil
}

func (r *registryNoop) Set(key Key, fp any) {
}

func (r *registryNoop) Delete(key Key) {
}

func (r *registryNoop) Keys() []Key {
	return nil
}
