// Package draft omits disabled draft fields from affected protobuf trees.
package draft

import (
	"fmt"
	"slices"
	"sync"

	"google.golang.org/protobuf/proto"
)

type Selection struct {
	once     sync.Once
	load     func() []string
	features []string
}

func NewSelection(load func() []string) *Selection { return &Selection{load: load} }

func (s *Selection) Enabled(feature string) bool {
	s.once.Do(func() { s.features = s.load() })
	return slices.Contains(s.features, feature)
}

type draftOmitter interface {
	OmitDraftFields(enabled func(string) bool)
}

// Omit adapts the SaaS sanitizer middleware's generated-interface dispatch.
func Omit(msg any, s *Selection) (affected bool, err error) {
	omitter, ok := msg.(draftOmitter)
	if !ok {
		return false, nil
	}
	affected = true
	defer func() {
		if recovered := recover(); recovered != nil {
			if message, ok := msg.(proto.Message); ok {
				err = fmt.Errorf("draft field omission failed for %s: %v", proto.MessageName(message), recovered)
			} else {
				err = fmt.Errorf("draft field omission failed for %T: %v", msg, recovered)
			}
		}
	}()
	omitter.OmitDraftFields(s.Enabled)
	return true, nil
}
