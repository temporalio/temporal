package useexp

import example "go.temporal.io/api/example/v1"

func flagged(req *example.Request) {
	_ = &example.Request{NewField: "x"} // want `experimental API field Request.NewField`
	_ = req.NewField                    // want `experimental API field Request.NewField`
	_ = req.GetNewField()               // want `experimental API field Request.NewField`
	_ = example.STATUS_EXPERIMENTAL     // want `experimental API enum value temporal.api.example.v1.Status.STATUS_EXPERIMENTAL`
}

func allowed(req *example.Request) {
	// temporal:allow-experimental-api experiment-a
	_ = req.NewField
	// temporal:allow-experimental-api experiment-b
	_ = example.STATUS_EXPERIMENTAL
}

func structLiteralOverrideRejected() {
	// temporal:allow-experimental-api experiment-a // want `unused experimental API allow directive`
	_ = &example.Request{NewField: "x"} // want `experimental API field Request.NewField.*cannot be set in a struct literal`
}

func staleDirective(req *example.Request) {
	// temporal:allow-experimental-api experiment-a // want `unused experimental API allow directive`
	_ = req.StableField
}

func wrongExperiment(req *example.Request) {
	// temporal:allow-experimental-api experiment-b // want `unused experimental API allow directive`
	_ = req.NewField // want `experimental API field Request.NewField`
}
