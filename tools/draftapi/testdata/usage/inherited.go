package usage

import (
	pb "go.temporal.io/api/testdraft/v1"
	refs "go.temporal.io/api/testreferences/v1"
)

func inherited(req *pb.Stable, cross *refs.References) {
	_ = req.Whole              // want "use of draft API .*message-feature"
	_ = req.GetWhole()         // want "use of draft API .*message-feature"
	_ = req.GetList()          // want "use of draft API .*message-feature"
	_ = req.GetByName()        // want "use of draft API .*message-feature"
	_ = req.GetInheritedEnum() // want "use of draft API .*message-feature"
	_ = req.GetDraftChild()    // want "use of draft API .*message-feature"
	_ = pb.Stable_DraftChild{} // want "use of draft API .*message-feature"
	_ = req.GetMixed()
	_ = req.GetOrdinary()
	_ = pb.Stable_Ordinary{}
	_ = cross.GetValue() // want "use of draft API .*message-feature"
	_ = cross.List       // want "use of draft API .*message-feature"
	_ = cross.ByName     // want "use of draft API .*message-feature"
	_ = cross.GetState() // want "use of draft API .*message-feature"
	_ = cross.GetChild() // want "use of draft API .*message-feature"
	_ = cross.GetStable()
	_ = refs.References_Child{} // want "use of draft API .*message-feature"
}

func oneFeature(req *pb.Stable) {
	_ = req.GetAlsoDraft()   // want "use of draft API .*message-feature"
	_ = req.GetAlsoDraft()   // temporal:allow-draft-api field-feature // want "use of draft API .*message-feature"
	_ = req.GetAlsoDraft()   // temporal:allow-draft-api message-feature
	_ = req.GetNestedDraft() // want "use of draft API .*message-feature"
	_ = req.GetNestedDraft() // temporal:allow-draft-api message-feature
}
