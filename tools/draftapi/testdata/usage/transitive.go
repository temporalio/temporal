package usage

import "go.temporal.io/server/bridge"

func transitive(req *bridge.Alias, embedded *bridge.Embedded) {
	_ = req.Draft           // want "use of draft API .*field-feature"
	_ = embedded.Draft      // want "use of draft API .*field-feature"
	_ = embedded.GetDraft() // want "use of draft API .*field-feature"
}
