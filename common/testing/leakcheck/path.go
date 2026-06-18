package leakcheck

import (
	"fmt"
	"strings"
)

type pathSegmentKind int

const (
	pathSegmentField pathSegmentKind = iota
	pathSegmentIndex
	pathSegmentMapKey
)

type objectPath []pathSegment

type pathSegment struct {
	kind  pathSegmentKind
	value string
}

func newObjectPath(root string) objectPath {
	return objectPath{{kind: pathSegmentField, value: root}}
}

func (p objectPath) Field(name string) objectPath {
	return p.append(pathSegment{kind: pathSegmentField, value: name})
}

func (p objectPath) Index(index int) objectPath {
	return p.append(pathSegment{kind: pathSegmentIndex, value: fmt.Sprint(index)})
}

func (p objectPath) MapKey(index int) objectPath {
	return p.append(pathSegment{kind: pathSegmentMapKey, value: fmt.Sprint(index)})
}

func (p objectPath) append(segment pathSegment) objectPath {
	next := make(objectPath, 0, len(p)+1)
	next = append(next, p...)
	next = append(next, segment)
	return next
}

func (p objectPath) String() string {
	return p.format(false)
}

func (p objectPath) Normalized() string {
	return p.format(true)
}

func (p objectPath) format(normalized bool) string {
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
		}
	}
	return out.String()
}
