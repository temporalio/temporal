package bridge

import pb "go.temporal.io/api/testdraft/v1"

type Alias = pb.Stable

type Embedded struct{ pb.Stable }

// DraftChoice aliases the draft oneof wrapper used by the analyzer fixture.
// temporal:allow-draft-api branch-feature
type DraftChoice = pb.Stable_DraftChoice
