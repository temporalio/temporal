package lint

import "github.com/quasilyte/go-ruleguard/dsl"

func staticThrottledLogMessages(m dsl.Matcher) {
	m.Import("go.temporal.io/server/common/log")

	// Named throttled fields and accessors may expose only log.Logger.
	m.Match(`$logger.$method($message, $*tags)`,
		`$loggerType.$method($logger, $message, $*tags)`).
		Where(m["method"].Text.Matches(`^(Debug|Info|Warn|Error)$`) &&
			m["message"].Type.Is("string") && !m["message"].Const &&
			(m["logger"].Type.Is("log.ThrottledLogger") ||
				m["logger"].Type.Is("*log.throttledLogger") ||
				(m["logger"].Type.Is("log.Logger") &&
					m["logger"].Text.Matches(`(?i)(^|\.)(get)?throttledlogger(\(\))?$`)))).
		At(m["message"]).
		Report("throttled log message must be a compile-time string constant; move dynamic details into tags")
}
