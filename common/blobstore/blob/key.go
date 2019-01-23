// Copyright (c) 2017 Uber Technologies, Inc.
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

package blob

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	// piecesSeparator is used to separate different sections of a key name
	piecesSeparator = "_"
	// keySizeLimit indicates the max length of a key including separator tokens and extension
	keySizeLimit = 255
	// piecesLimit indicates the limit on the number of separate pieces that can be used to construct a key name
	piecesLimit = 4
	// extensionSeparator indicates the token used to separate key name from key extension
	extensionSeparator = "."
)

var (
	// allowedRegex indicates the allowed format of both key name pieces and extension
	allowedRegex = regexp.MustCompile(`^[a-zA-Z0-9]+$`)
)

type (
	// Key identifies a blob
	Key interface {
		String() string
		Pieces() []string
		Extension() string
	}
	key struct {
		str       string
		pieces    []string
		extension string
	}
)

// NewKey constructs a new valid key or returns error on any input that produces an invalid key
// A valid key is of form foo_bar_baz_raz.ext
// The section before the period is considered the key name and the piece after the period is considered the extension
// Pieces are combined to form the key name and pieces are separated by underscores
// Keys are immutable
func NewKey(extension string, pieces ...string) (Key, error) {
	if len(pieces) == 0 {
		return nil, errors.New("must give at least one piece")
	}
	if len(pieces) > piecesLimit {
		return nil, fmt.Errorf("number of pieces given exceeds limit of %v", piecesLimit)
	}
	if !allowedRegex.MatchString(extension) {
		return nil, fmt.Errorf("extension, %v, contained illegal characters - allowed characters are %v", extension, allowedRegex.String())
	}
	for _, p := range pieces {
		if !allowedRegex.MatchString(p) {
			return nil, fmt.Errorf("piece, %v, contained illegal characters - allowed characters are %v", p, allowedRegex.String())
		}
	}
	name := strings.Join(pieces, piecesSeparator)
	str := strings.Join([]string{name, extension}, extensionSeparator)
	if len(str) > keySizeLimit {
		return nil, fmt.Errorf("produced key size of %v greater than limit of %v", len(str), keySizeLimit)
	}
	return &key{
		str:       str,
		pieces:    pieces,
		extension: extension,
	}, nil
}

// NewKeyFromString constructs a valid key from string or returns error if input produces invalid key
func NewKeyFromString(str string) (Key, error) {
	keyParts := strings.Split(str, extensionSeparator)
	if len(keyParts) != 2 {
		return nil, fmt.Errorf("%v is invalid exactly one %q should exist in key", str, extensionSeparator)
	}
	namePieces := strings.Split(keyParts[0], piecesSeparator)
	extension := keyParts[1]
	return NewKey(extension, namePieces...)
}

// String returns the built string representation of key of form foo_bar_baz_raz.ext
func (k *key) String() string {
	return k.str
}

// Pieces returns a slice of the individual pieces used to construct the key name (this does not include the extension)
func (k *key) Pieces() []string {
	return k.pieces
}

// Extension returns the extension used to construct the key
func (k *key) Extension() string {
	return k.extension
}
