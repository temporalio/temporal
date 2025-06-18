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
	rootPath = []string{}
)

// The Encode method encodes node path in a way that the following uses cases can be
// achieved by doing a simple a range query in DB based on prefixes of the encoded path:
// 1. Getting all nodes for a chasm tree.
// 2. Getting all nodes for a sub-tree.
// 3. Getting all immediate children of a Collection node.
// Additionally, it allows getting all ancestor nodes of a given node.
//
// It does so by using a different separator for a node which is a direct child of a Collection node.
// The two separators used ('$' and '#') are next to each other in terms of values, which ensures all
// children for a node are grouped together. Additionally, all immediate children of a Collection node
// are grouped together as well.
//
// To get a sub-tree (say a node with name "foo"), we want to use a query look like the following:
//
//	path >= "foo" AND path < "foo[something]"
//
// and we need to know the minimal value of [something] that can possibly be in the encoded path.
// This is achieved by escaping '\', '$', '#', and also every code point less than '#'.
// Since the escaped character itself is larger than '#', the minimal value is '%' and our query becomes:
//
//	path >= "foo" AND path < "foo%"
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

	path := make([]string, 0, 3)
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
			path = append(path, b.String())
			b.Reset()
			continue
		}

		_, _ = b.WriteRune(r)
	}
	if escaped {
		return nil, serviceerror.NewInternalf("encoded path ends with escape character: %v", encodedPath)
	}

	path = append(path, b.String())
	return path, nil
}
