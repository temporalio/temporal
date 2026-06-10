package persistence

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
)

const (
	queryDelimiter        = ';'
	querySliceDefaultSize = 100

	sqlLeftParenthesis     = '('
	sqlRightParenthesis    = ')'
	sqlCreateKeyword       = "create"
	sqlTableKeyword        = "table"
	sqlIndexKeyword        = "index"
	sqlConcurrentlyKeyword = "concurrently"
	sqlAddKeyword          = "add"
	sqlColumnKeyword       = "column"
	sqlDoubleDollarKeyword = "$$"
	sqlIfKeyword           = "if"
	sqlLoopKeyword         = "loop"
	sqlBeginKeyword        = "begin"
	sqlEndKeyword          = "end"
	sqlLineComment         = "--"
	sqlSingleQuote         = '\''
	sqlDoubleQuote         = '"'
)

// LoadAndSplitQuery loads and split cql / sql query into one statement per string.
// Comments are removed from the query.
func LoadAndSplitQuery(
	filePaths []string,
) ([]string, error) {
	var files []io.Reader

	for _, filePath := range filePaths {
		f, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("error opening file %s: %w", filePath, err)
		}
		files = append(files, f)
	}

	return LoadAndSplitQueryFromReaders(files)
}

// LoadAndSplitQueryFromReaders loads and split cql / sql query into one statement per string.
// Comments are removed from the query.
func LoadAndSplitQueryFromReaders(
	readers []io.Reader,
) ([]string, error) {
	result := make([]string, 0, querySliceDefaultSize)
	for _, r := range readers {
		content, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("error reading contents: %w", err)
		}
		n := len(content)
		contentStr := string(bytes.ToLower(content))
		for i, j := 0, 0; i < n; i = j {
			// stack to keep track of open parenthesis/blocks
			var st []byte
			var stmtBuilder strings.Builder

		stmtLoop:
			for ; j < n; j++ {
				switch contentStr[j] {
				case queryDelimiter:
					if len(st) == 0 {
						j++
						break stmtLoop
					}

				case sqlLeftParenthesis:
					st = append(st, sqlLeftParenthesis)

				case sqlRightParenthesis:
					if len(st) == 0 || st[len(st)-1] != sqlLeftParenthesis {
						return nil, fmt.Errorf("error reading contents: unmatched right parenthesis")
					}
					st = st[:len(st)-1]

				case sqlDoubleDollarKeyword[0]:
					if !hasWordAt(contentStr, sqlDoubleDollarKeyword, j) {
						continue
					}
					if len(st) == 0 || st[len(st)-1] != sqlDoubleDollarKeyword[0] {
						st = append(st, sqlDoubleDollarKeyword[0])
						j += len(sqlDoubleDollarKeyword) - 1
					} else {
						st = st[:len(st)-1]
						j += len(sqlDoubleDollarKeyword) - 1
					}

				case sqlIfKeyword[0]:
					if !hasWordAt(contentStr, sqlIfKeyword, j) {
						continue
					}
					if hasWordsBefore(contentStr, j-1, sqlAddKeyword, sqlColumnKeyword) ||
						hasWordsBefore(contentStr, j-1, sqlCreateKeyword, sqlIndexKeyword) ||
						hasWordsBefore(contentStr, j-1, sqlCreateKeyword, sqlIndexKeyword, sqlConcurrentlyKeyword) ||
						hasWordsBefore(contentStr, j-1, sqlCreateKeyword, sqlTableKeyword) {
						continue
					}
					st = append(st, sqlIfKeyword[0])
					j += len(sqlIfKeyword) - 1

				case sqlLoopKeyword[0]:
					if !hasWordAt(contentStr, sqlLoopKeyword, j) {
						continue
					}
					st = append(st, sqlLoopKeyword[0])
					j += len(sqlLoopKeyword) - 1

				case sqlBeginKeyword[0]:
					if hasWordAt(contentStr, sqlBeginKeyword, j) {
						st = append(st, sqlBeginKeyword[0])
						j += len(sqlBeginKeyword) - 1
					}

				case sqlEndKeyword[0]:
					if !hasWordAt(contentStr, sqlEndKeyword, j) {
						continue
					}
					if ok, after := hasWordAfter(contentStr, sqlIfKeyword, j+len(sqlEndKeyword)); ok {
						if len(st) == 0 || st[len(st)-1] != sqlIfKeyword[0] {
							return nil, errors.New("error reading contents: unmatched `END IF` keyword")
						}
						st = st[:len(st)-1]
						j = after + len(sqlIfKeyword) - 1
					} else if ok, after := hasWordAfter(contentStr, sqlLoopKeyword, j+len(sqlEndKeyword)); ok {
						//nolint:revive
						if len(st) == 0 || st[len(st)-1] != sqlLoopKeyword[0] {
							return nil, errors.New("error reading contents: unmatched `END LOOP` keyword")
						}
						st = st[:len(st)-1]
						j = after + len(sqlLoopKeyword) - 1
					} else {
						if len(st) == 0 || st[len(st)-1] != sqlBeginKeyword[0] {
							return nil, errors.New("error reading contents: unmatched `END` keyword")
						}
						st = st[:len(st)-1]
						j += len(sqlEndKeyword) - 1
					}

				case sqlSingleQuote, sqlDoubleQuote:
					quote := contentStr[j]
					j++
					for j < n && contentStr[j] != quote {
						j++
					}
					if j == n {
						return nil, fmt.Errorf("error reading contents: unmatched quotes")
					}

				case sqlLineComment[0]:
					if j+len(sqlLineComment) <= n && contentStr[j:j+len(sqlLineComment)] == sqlLineComment {
						_, _ = stmtBuilder.Write(bytes.TrimRight(content[i:j], " "))
						for j < n && contentStr[j] != '\n' {
							j++
						}
						i = j
					}

				default:
					// no-op: generic character
				}
			}

			if len(st) > 0 {
				switch st[len(st)-1] {
				case sqlLeftParenthesis:
					return nil, fmt.Errorf("error reading contents: unmatched left parenthesis")
				case sqlBeginKeyword[0]:
					return nil, fmt.Errorf("error reading contents: unmatched `BEGIN` keyword")
				default:
					// should never enter here
					return nil, fmt.Errorf("error reading contents: unmatched `%c`", st[len(st)-1])
				}
			}

			_, _ = stmtBuilder.Write(content[i:j])
			stmt := strings.TrimSpace(stmtBuilder.String())
			if stmt == "" {
				continue
			}
			result = append(result, stmt)
		}
	}
	return result, nil
}

// hasWordAt is a simple test to check if it matches the whole word:
// it checks if the adjacent characters are not alphanumeric if they exist.
func hasWordAt(s, word string, pos int) bool {
	if pos+len(word) > len(s) || s[pos:pos+len(word)] != word {
		return false
	}
	if pos > 0 && isAlphanumeric(s[pos-1]) {
		return false
	}
	if pos+len(word) < len(s) && isAlphanumeric(s[pos+len(word)]) {
		return false
	}
	return true
}

// hasWordAfter checks if the given word appears after position pos in s,
// separated by at least one space, and is a whole word.
func hasWordAfter(s, word string, pos int) (bool, int) {
	after := pos
	for after < len(s) && unicode.IsSpace(rune(s[after])) {
		after++
	}
	if after == pos {
		return false, after
	}
	return hasWordAt(s, word, after), after
}

// hasWordsBefore checks if the given words appears before position pos in s,
// separated by at least one space, and each word is a whole word.
// Words must appear is the given order.
// Eg: hasWordsBefore("CREATE TABLE IF", 12, "CREATE", "TABLE") returns true.
func hasWordsBefore(s string, pos int, words ...string) bool {
	if len(words) == 0 {
		return true
	}
	if pos <= 0 || !unicode.IsSpace(rune(s[pos])) {
		return false
	}
	for i := len(words) - 1; i >= 0; i-- {
		// skip spaces
		for pos >= 0 && unicode.IsSpace(rune(s[pos])) {
			pos--
		}
		// go to first char of word
		for pos > 0 && isAlphanumeric(s[pos-1]) {
			pos--
		}
		// check if current pos matches the word
		if pos < 0 || !hasWordAt(s, words[i], pos) {
			return false
		}
		pos--
	}
	return true
}

func isAlphanumeric(c byte) bool {
	return unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c))
}
