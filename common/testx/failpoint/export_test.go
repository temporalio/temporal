package failpoint

// Exports of internals for the black-box tests in package failpoint_test.

// State is the observable configuration of a failpoint.
type State struct {
	Key     AnyKey
	Action  Action
	Trigger Trigger
}

// State returns the configuration of the failpoint.
func (f *Failpoint[T]) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return State{
		Key:     f.key,
		Action:  f.action,
		Trigger: f.trigger,
	}
}

// UndeclaredKey returns a key that no NewKey declared, for tests of the failpoint mechanics.
func UndeclaredKey[T any](name string) Key[T] {
	return Key[T]{name: name}
}
