package usage

import pb "go.temporal.io/api/testdraft/v1"

func testHelper(req *pb.Stable) { _ = req.Draft }
