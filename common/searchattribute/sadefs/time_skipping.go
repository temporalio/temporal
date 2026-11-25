package sadefs

// The first character represents enabled, and the second represents positive accumulated skipped duration.
// SQL columns reserve space beyond this layout. Extending it requires readers and query conversion
// to support both old and new encodings before writers emit the new format.
const (
	// TimeSkippingStateDisabledUnused means time skipping is disabled and no duration has been skipped.
	TimeSkippingStateDisabledUnused = "FF"
	// TimeSkippingStateDisabledUsed means time skipping is disabled and duration has been skipped.
	TimeSkippingStateDisabledUsed = "FT"
	// TimeSkippingStateEnabledUnused means time skipping is enabled and no duration has been skipped.
	TimeSkippingStateEnabledUnused = "TF"
	// TimeSkippingStateEnabledUsed means time skipping is enabled and duration has been skipped.
	TimeSkippingStateEnabledUsed = "TT"
)
