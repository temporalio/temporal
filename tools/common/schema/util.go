package schema

import (
	"bufio"
	"io"
	"os"
	"strings"
)

const newLineDelim = '\n'

// ParseFile takes a cql / sql file path as input
// and returns an array of cql / sql statements on
// success.
func ParseFile(filePath string) ([]string, error) {
	// #nosec
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(f)

	var line string
	var currStmt string
	var stmts = make([]string, 0, 4)

	for err == nil {

		line, err = reader.ReadString(newLineDelim)
		line = strings.TrimSpace(line)
		if len(line) < 1 {
			continue
		}

		// Filter out the comment lines, the
		// only recognized comment line format
		// is any line that starts with double dashes
		tokens := strings.Split(line, "--")
		if len(tokens) > 0 && len(tokens[0]) > 0 {
			currStmt += tokens[0]
			// semi-colon is the end of statement delim
			if strings.HasSuffix(currStmt, ";") {
				stmts = append(stmts, currStmt)
				currStmt = ""
			}
		}
	}

	if err == io.EOF {
		return stmts, nil
	}

	return nil, err
}
