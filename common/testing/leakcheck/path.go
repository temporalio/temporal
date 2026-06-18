package leakcheck

import (
	"slices"
	"strconv"
	"strings"
)

type pathSegmentKind int

const (
	pathSegmentField pathSegmentKind = iota
	pathSegmentIndex
	pathSegmentMapKey
)

type path []pathSegment

type pathSegment struct {
	kind  pathSegmentKind
	value string
}

func (p path) field(name string) path {
	return p.append(pathSegment{kind: pathSegmentField, value: name})
}

func (p path) index(index int) path {
	return p.append(pathSegment{kind: pathSegmentIndex, value: strconv.Itoa(index)})
}

func (p path) mapKey(index int) path {
	return p.append(pathSegment{kind: pathSegmentMapKey, value: strconv.Itoa(index)})
}

func (p path) append(segment pathSegment) path {
	// Each recursion branch owns its path so sibling fields cannot mutate it.
	return append(slices.Clone(p), segment)
}

func (p path) normalized() string {
	return p.format(true)
}

func (p path) format(normalized bool) string {
	var out strings.Builder
	for i, segment := range p {
		switch segment.kind {
		case pathSegmentField:
			if i > 0 {
				out.WriteByte('.')
			}
			out.WriteString(segment.value)
		case pathSegmentIndex:
			if normalized {
				out.WriteString("[*]")
			} else {
				out.WriteByte('[')
				out.WriteString(segment.value)
				out.WriteByte(']')
			}
		case pathSegmentMapKey:
			if normalized {
				out.WriteString("[key*]")
			} else {
				out.WriteString("[key")
				out.WriteString(segment.value)
				out.WriteByte(']')
			}
		default:
			continue
		}
	}
	return out.String()
}
