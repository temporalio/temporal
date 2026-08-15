package verify

type TraceVocabulary struct {
	Actions      map[string]string
	Bindings     map[string]map[string]string
	Properties   map[string][]string
	EntityExists map[string]string
	EntityStates map[string]string
	Relations    map[string]string
	Identities   map[string]string
	States       map[string]string
}
