package temporalzfs

import "go.temporal.io/server/chasm"

var statusSearchAttribute = chasm.NewSearchAttributeKeyword(
	"FilesystemStatus",
	chasm.SearchAttributeFieldLowCardinalityKeyword01,
)
