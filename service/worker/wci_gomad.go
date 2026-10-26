//go:build gomad

package worker

import "go.uber.org/fx"

// The auto-scaled-workers component reaches os/exec through its compute
// providers and k8s client; the gomad build runs the worker without it.
var wciComponentModule = fx.Options()
