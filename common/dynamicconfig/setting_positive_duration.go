package dynamicconfig

import (
	"fmt"
	"time"
)

// convertPositiveDuration is convertDuration, but rejects values <= 0. A rejected value makes
// the collection log a warning and use the setting's default instead.
func convertPositiveDuration(val any) (time.Duration, error) {
	d, err := convertDuration(val)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("duration must be positive, got %v", d)
	}
	return d, nil
}

// NewGlobalPositiveDurationSetting is like NewGlobalDurationSetting, but a configured value <= 0
// is ignored (with a warning) and the default is used. Use it for intervals where zero does not
// mean "disabled" and would instead spin a loop, panic a ticker, or multiply the work done.
func NewGlobalPositiveDurationSetting(key string, def time.Duration, description string) GlobalDurationSetting {
	return NewGlobalTypedSettingWithConverter[time.Duration](key, convertPositiveDuration, def, description)
}

// NewTaskQueuePositiveDurationSettingWithConstrainedDefault is like
// NewTaskQueueDurationSettingWithConstrainedDefault, but a configured value <= 0 is ignored (with a
// warning) and the default is used.
func NewTaskQueuePositiveDurationSettingWithConstrainedDefault(key string, cdef []TypedConstrainedValue[time.Duration], description string) TaskQueueDurationConstrainedDefaultSetting {
	return NewTaskQueueTypedSettingWithConstrainedDefault[time.Duration](key, convertPositiveDuration, cdef, description)
}
