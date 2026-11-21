// temporal:allow-draft-api field-feature
package usage

import pb "go.temporal.io/api/testdraft/v1"

func entireFile(req *pb.Stable) { _ = req.Draft }
