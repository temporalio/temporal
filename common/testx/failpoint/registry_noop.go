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

func (r *registryNoop) GetOrSet(key Key, fp any) any {
	return fp
}

func (r *registryNoop) Delete(key Key) {
}

func (r *registryNoop) CompareAndDelete(key Key, fp any) bool {
	return false
}

func (r *registryNoop) DeleteAll() []any {
	return nil
}

func (r *registryNoop) Configure(fn func()) {
	fn()
}

func (r *registryNoop) ID() string {
	return ""
}

func (r *registryNoop) Keys() []Key {
	return nil
}

func (r *registryNoop) Subscribe(func(Event)) func() {
	return func() {}
}

func (r *registryNoop) emit(Event) {}
