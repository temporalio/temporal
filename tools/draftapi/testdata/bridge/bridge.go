package bridge

import pb "go.temporal.io/api/testdraft/v1"

type Alias = pb.Stable

type Embedded struct{ pb.Stable }
