package testrunner

import (
	"strings"
	"unicode/utf8"

	"go.temporal.io/server/tools/common/junit"
)

const testrunnerSuiteName = "testrunner"
const junitDetailsMaxBytes = 64 * 1024

type failureType string

const (
	// failureTypeFailed marks a failed assertion.
	failureTypeFailed   failureType = "Failed"
	failureTypeAborted  failureType = "ABORTED"
	failureTypeTimeout  failureType = "TIMEOUT"
	failureTypeCrash    failureType = "CRASH"
	failureTypeDataRace failureType = "DATA RACE"
	failureTypePanic    failureType = "PANIC"
	failureTypeFatal    failureType = "FATAL"
)

func generateFailure(kind failureType, data string) *junit.Result {
	return &junit.Result{
		Message: string(kind),
		Type:    string(kind),
		Data:    data,
	}
}

// sanitizeXML removes characters that are invalid in XML 1.0. Go's xml.Encoder
// escapes <, >, & etc., but control characters other than \t, \n, \r are not
// legal XML and cause parsers to reject the document.
func sanitizeXML(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\t', '\n', '\r':
			return r
		case 0xFFFE, 0xFFFF:
			return -1 // Reserved Unicode noncharacters; disallowed in XML 1.0.
		}
		if r < 0x20 {
			// 0x20 is space; lower code points are ASCII control characters.
			return -1
		}
		return r
	}, s)
}

// truncateDetails keeps failure payloads from bloating the JUnit artifact.
func truncateDetails(s string) string {
	if len(s) <= junitDetailsMaxBytes {
		return s
	}
	const marker = "\n... (truncated) ...\n"
	end := junitDetailsMaxBytes - len(marker)
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + marker
}
