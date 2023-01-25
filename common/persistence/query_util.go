// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package persistence

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
)

const (
	queryDelimiter        = ';'
	querySliceDefaultSize = 100

	sqlLeftParenthesis  = '('
	sqlRightParenthesis = ')'
	sqlBeginKeyword     = "begin"
	sqlEndKeyword       = "end"
	sqlLineComment      = "--"
	sqlSingleQuote      = '\''
	sqlDoubleQuote      = '"'
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

				case sqlBeginKeyword[0]:
					if hasWordAt(contentStr, sqlBeginKeyword, j) {
						st = append(st, sqlBeginKeyword[0])
						j += len(sqlBeginKeyword) - 1
					}

				case sqlEndKeyword[0]:
					if hasWordAt(contentStr, sqlEndKeyword, j) {
						if len(st) == 0 || st[len(st)-1] != sqlBeginKeyword[0] {
							return nil, fmt.Errorf("error reading contents: unmatched `END` keyword")
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
// it checks if the adjacent charactes are not alphanumeric if they exist.
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

func isAlphanumeric(c byte) bool {
	return unicode.IsLetter(rune(c)) || unicode.IsDigit(rune(c))
}
