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
	_ = req.DraftUnion                    // want "use of draft API .*oneof-feature"
	_ = req.GetDraftUnion()               // want "use of draft API .*oneof-feature"
	_ = req.GetMember()                   // want "use of draft API .*oneof-feature"
	_ = pb.Stable_Member{Member: "value"} // want "use of draft API .*oneof-feature" "use of draft API .*oneof-feature"
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
	_ = pb.StableEnum_DRAFT_VALUE // want "use of draft API .*value-feature"
	_ = pb.StableEnum_DRAFT_ALIAS // want "use of draft API .*alias-feature"
	_ = pb.DraftEnum_DRAFT_ZERO   // want "use of draft API .*enum-feature"
	var _ pb.DraftEnum            // want "use of draft API .*enum-feature"
}

func methods(ctx context.Context, client pb.StableServiceClient) {
	_, _ = client.StableCall(ctx, nil)
	_, _ = client.DraftCall(ctx, nil)             // want "use of draft API .*method-feature"
	_ = pb.StableService_DraftCall_FullMethodName // want "use of draft API .*method-feature"
	_ = pb.NewDraftServiceClient                  // want "use of draft API .*service-feature"
	var _ pb.DraftServiceClient                   // want "use of draft API .*service-feature"
	var _ pb.DraftServiceServer                   // want "use of draft API .*service-feature"
	_ = pb.RegisterDraftServiceServer             // want "use of draft API .*service-feature"
	_ = pb.UnimplementedDraftServiceServer{}      // want "use of draft API .*service-feature"
	_ = pb.DraftService_ServiceDesc               // want "use of draft API .*service-feature"
	var _ pb.EmptyDraftServiceClient              // want "use of draft API .*empty-service-feature"
	_ = pb.NewEmptyDraftServiceClient             // want "use of draft API .*empty-service-feature"
	var server pb.UnimplementedStableServiceServer
	_, _ = server.DraftCall(ctx, nil) // want "use of draft API .*method-feature"
}

type lookalike struct{ Draft string }

func unrelated(req lookalike) { _ = req.Draft }
