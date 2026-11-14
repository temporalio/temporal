package usage

import pb "go.temporal.io/api/testdraft/v1"

// temporal:allow-draft-api field-feature // Feature is gated by the caller.
func wholeFunction(req *pb.Stable) {
	_ = req.Draft
	_ = req.GetDraft()
}

func statements(req *pb.Stable) {
	_ = req.Draft // temporal:allow-draft-api field-feature
	// temporal:allow-draft-api field-feature
	_ = req.GetDraft()
	_ = req.Draft // temporal:allow-draft-api wrong-feature // want "use of draft API .*field-feature"
	_ = req.Draft // temporal:allow-draft-api Field-Feature // want "use of draft API .*field-feature"
	_ = req.Draft // temporal:allow-draft-api * // want "use of draft API .*field-feature"
	_ = req.Draft //nolint:draftapi // want "use of draft API .*field-feature"
	_ = req.Draft //nolint:all // want "use of draft API .*field-feature"
	_ = req.Draft //nolint // want "use of draft API .*field-feature"
	// Mentioning temporal:allow-draft-api field-feature in prose is not a directive.
	_ = req.Draft // want "use of draft API .*field-feature"
	// temporal:allow-draft-apix field-feature
	_ = req.Draft // want "use of draft API .*field-feature"
	// temporal:allow-draft-api
	_ = req.Draft // want "use of draft API .*field-feature"
	// temporal:allow-draft-api field-feature

	_ = req.Draft // want "use of draft API .*field-feature"
}

// A trailing directive covers only its own line, including on a line that opens a block.
func funcOpener(req *pb.Stable) { // temporal:allow-draft-api field-feature
	_ = req.Draft // want "use of draft API .*field-feature"
}

func blockOpeners(reqs []*pb.Stable, ok bool, n int) {
	for _, req := range reqs { // temporal:allow-draft-api field-feature
		if ok { // temporal:allow-draft-api field-feature
			for range n {
				_ = req.Draft // want "use of draft API .*field-feature"
			}
		} else { // temporal:allow-draft-api field-feature
			_ = req.Draft // want "use of draft API .*field-feature"
		}
		switch n {
		case 1: // temporal:allow-draft-api field-feature
			_ = req.Draft // want "use of draft API .*field-feature"
		default: // temporal:allow-draft-api field-feature
			_ = req.Draft // want "use of draft API .*field-feature"
		}
		go func() { // temporal:allow-draft-api field-feature
			_ = req.Draft // want "use of draft API .*field-feature"
		}()
		_ = []string{ // temporal:allow-draft-api field-feature
			req.Draft, // want "use of draft API .*field-feature"
		}
	}
}

func nextLine(req *pb.Stable) {
	_ = 1         // temporal:allow-draft-api field-feature
	_ = req.Draft // want "use of draft API .*field-feature"
}

// temporal:allow-draft-api message-feature
type DraftAlias = pb.Draft

// temporal:allow-draft-api message-feature
var global = pb.Draft{}
