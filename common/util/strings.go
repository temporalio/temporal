package util

import "unicode/utf8"

// TruncateUTF8 truncates s to no more than n _bytes_, and returns a valid utf-8 string as long
// as the input is a valid utf-8 string.
// Note that truncation pays attention to codepoints only! This may truncate in the middle of a
// grapheme cluster.
func TruncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	} else if n <= 0 {
		return ""
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
