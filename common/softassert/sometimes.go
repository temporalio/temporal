package softassert

// Sometimes declares a path that should be reached in some execution. The
// message must be a string literal. Ordinary builds evaluate the argument and
// perform no other work. Gomad builds run the same body: gomad observes the
// calls with --observe-call=go.temporal.io/server/common/softassert.Sometimes,
// which needs no build tag, generated catalog or source binding.
func Sometimes(staticMessage string) {}
