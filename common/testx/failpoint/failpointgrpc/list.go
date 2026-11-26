package failpointgrpc

import (
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/protobuf/proto"
)

// defaultListLimit is the size, in bytes, that a ListFailpoints response is kept within. It is below
// the 4 MiB that a gRPC client receives by default, so a registry with many or long key names
// still lists instead of failing the call.
const defaultListLimit = 3 << 20

// listEntryOverhead is what a list entry adds to its own size: its tag and its length.
const listEntryOverhead = 6

// limitList removes entries from the end of the failpoints and then the fallback keys of resp until
// they fit in limit bytes, and adds the number of entries it removed to the omitted counts of resp.
// The failpoints come first: they are what a test configured.
func limitList(resp *failpointpb.ListFailpointsResponse, limit int) {
	budget := limit
	var omitted int64
	resp.Failpoints, omitted = fit(resp.Failpoints, &budget)
	resp.FailpointsOmitted += omitted
	resp.FallbackKeys, omitted = fit(resp.FallbackKeys, &budget)
	resp.FallbackKeysOmitted += omitted
}

// fit returns the longest prefix of entries that fits in budget, and the number of entries that it
// leaves out. It subtracts the size of the prefix from budget.
func fit[T proto.Message](entries []T, budget *int) ([]T, int64) {
	for i, entry := range entries {
		size := proto.Size(entry) + listEntryOverhead
		if size > *budget {
			// Nothing after the first entry that doesn't fit is listed, so the list is a prefix.
			*budget = 0
			return entries[:i], int64(len(entries) - i)
		}
		*budget -= size
	}
	return entries, 0
}
