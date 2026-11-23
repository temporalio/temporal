package testing

import pb "go.temporal.io/api/testdraft/v1"

func Draft(req *pb.Stable) { _ = req.Draft }
