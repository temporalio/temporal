package usage

import (
	"context"

	pb "go.temporal.io/api/testdraft/v1"
)

type Alias = pb.Stable

func fields(req *Alias) {
	_ = req.Stable
	_ = req.GetStable()
	_ = req.Draft          // want "use of draft API .*Stable.Draft .*field-feature"
	req.Draft = "value"    // want "use of draft API .*field-feature"
	_ = req.GetDraft()     // want "use of draft API .*field-feature"
	getter := req.GetDraft // want "use of draft API .*field-feature"
	_ = getter
	_ = pb.Stable{
		Draft: "value", // want "use of draft API .*field-feature"
	}
	_ = req.DraftMap                                // want "use of draft API .*map-feature"
	_ = req.GetDraftMap()                           // want "use of draft API .*map-feature"
	_ = req.GetDraftChoice()                        // want "use of draft API .*branch-feature"
	_ = pb.Stable_DraftChoice{DraftChoice: "value"} // want "use of draft API .*branch-feature" "use of draft API .*branch-feature"
	_ = pb.Stable_StableChoice{StableChoice: "value"}
	_ = req.GetChoice()
	_ = req.Choice
	_ = req.GetStableChoice()
}

func promoted() {
	var req struct{ pb.Stable }
	_ = req.Draft      // want "use of draft API .*field-feature"
	_ = req.GetDraft() // want "use of draft API .*field-feature"
}

func typesAndEnums() {
	_ = pb.Stable_StableNested{}
	_ = pb.Stable_DraftNested{} // want "use of draft API .*nested-feature"
	_ = pb.Draft{}              // want "use of draft API .*message-feature"
	_ = pb.Draft_Nested{}       // want "use of draft API .*message-feature"
	_ = pb.Draft_ZERO           // want "use of draft API .*message-feature"
	_ = pb.StableEnum_STABLE_ZERO
	_ = pb.StableEnum_STABLE_ALIAS
}

func methods(ctx context.Context, client pb.StableServiceClient) {
	_, _ = client.StableCall(ctx, nil)
	_ = pb.StableService_StableCall_FullMethodName
	var server pb.UnimplementedStableServiceServer
	_, _ = server.StableCall(ctx, nil)
}

type lookalike struct{ Draft string }

func unrelated(req lookalike) { _ = req.Draft }
