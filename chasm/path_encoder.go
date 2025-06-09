package chasm

import (
	"strings"
	"unicode/utf8"

	"go.temporal.io/api/serviceerror"
)

var _ NodePathEncoder = (*defaultPathEncoder)(nil)

var DefaultPathEncoder NodePathEncoder = &defaultPathEncoder{}

type defaultPathEncoder struct{}

const (
	nameSeparator       = '$'
	collectionSeparator = '#'
	escapeChar          = '\\'
)

var (
	rootPath []string
)

func (e *defaultPathEncoder) Encode(
	node *Node,
	path []string,
) (string, error) {
	if path == nil {
		path = node.path()
	}

	if len(path) == 0 {
		return "", nil
	}

	var b strings.Builder
	lastIdx := len(path) - 1
	for i, nodeName := range path {
		if i > 0 {
			if i == lastIdx &&
				node.parent != nil &&
				node.parent.serializedNode.GetMetadata().GetCollectionAttributes() != nil {
				_, _ = b.WriteRune(collectionSeparator)
			} else {
				_, _ = b.WriteRune(nameSeparator)
			}
		}

		if nodeName == "" {
			return "", serviceerror.NewInternalf("path contains empty node name: %v", path)
		}

		for _, r := range nodeName {
			if r == utf8.RuneError {
				return "", serviceerror.NewInvalidArgumentf("node name contains invalid UTF-8 code point: %v", nodeName)
			}

			if r == escapeChar ||
				r == nameSeparator ||
				r <= collectionSeparator {
				_, _ = b.WriteRune(escapeChar)
			}
			_, _ = b.WriteRune(r)
		}
	}
	return b.String(), nil
}

func (e *defaultPathEncoder) Decode(
	encodedPath string,
) ([]string, error) {
	if encodedPath == "" {
		return rootPath, nil
	}

	segments := make([]string, 0, 3)
	var b strings.Builder
	escaped := false
	for _, r := range encodedPath {
		if r == utf8.RuneError {
			return nil, serviceerror.NewInvalidArgumentf("encodedPath contains invalid UTF-8 code point: %v", encodedPath)
		}

		if escaped {
			_, _ = b.WriteRune(r)
			escaped = false
			continue
		}

		if r == '\\' {
			escaped = true
			continue
		}

		if r == '$' || r == '#' {
			segments = append(segments, b.String())
			b.Reset()
			continue
		}

		_, _ = b.WriteRune(r)
	}
	if escaped {
		return nil, serviceerror.NewInternalf("encoded path ends with escape character: %v", encodedPath)
	}

	segments = append(segments, b.String())
	return segments, nil
}
