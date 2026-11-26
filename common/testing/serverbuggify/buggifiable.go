package serverbuggify

import (
	"go.temporal.io/server/common/dynamicconfig"
)

// buggifiable lists the dynamic config settings that servers with failpoints enabled randomize
// when they run with BUGGIFY, with the alternative values that are legal in every deployment and
// likely to expose bugs. A BUGGIFY run picks one of the values of a setting for the whole run from
// its seed, unless dynamic config sets a value without constraints for the setting. The settings
// are only read differently if failpoints and BUGGIFY are enabled.
var buggifiable = []struct {
	setting dynamicconfig.GenericSetting
	values  []any
}{
	// Small task writer batches split concurrent task appends into several writes.
	{dynamicconfig.MatchingMaxTaskBatchSize, []any{1, 2, 10}},
	// Small batches make the timer queue page through its tasks and advance its reader often.
	{dynamicconfig.TimerTaskBatchSize, []any{1, 2, 10}},
	// Small batches make the transfer queue page through its tasks and advance its reader often.
	{dynamicconfig.TransferTaskBatchSize, []any{1, 2, 10}},
}

// BuggifiableSettings returns the alternative values of the settings that BUGGIFY randomizes, by
// key.
func BuggifiableSettings() map[dynamicconfig.Key][]any {
	settings := make(map[dynamicconfig.Key][]any, len(buggifiable))
	for _, b := range buggifiable {
		settings[b.setting.Key()] = b.values
	}
	return settings
}
