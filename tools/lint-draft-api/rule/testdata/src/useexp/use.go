package useexp

import example "go.temporal.io/api/example/v1"

func flagged(req *example.Request) {
	_ = &example.Request{NewField: "x"} // want `draft API field Request.NewField`
	_ = req.NewField                    // want `draft API field Request.NewField`
	_ = req.GetNewField()               // want `draft API field Request.NewField`
	_ = example.STATUS_DRAFT            // want `draft API enum value temporal.api.example.v1.Status.STATUS_DRAFT`
}

func allowed(req *example.Request) {
	// temporal:allow-draft-api draft-a
	_ = req.NewField
	// temporal:allow-draft-api draft-b
	_ = example.STATUS_DRAFT
}

func structLiteralOverrideRejected() {
	// temporal:allow-draft-api draft-a // want `unused draft API allow directive`
	_ = &example.Request{NewField: "x"} // want `draft API field Request.NewField.*cannot be set in a struct literal`
}

func staleDirective(req *example.Request) {
	// temporal:allow-draft-api draft-a // want `unused draft API allow directive`
	_ = req.StableField
}

func wrongDraft(req *example.Request) {
	// temporal:allow-draft-api draft-b // want `unused draft API allow directive`
	_ = req.NewField // want `draft API field Request.NewField`
}
