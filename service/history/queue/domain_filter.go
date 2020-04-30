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

package queue

// NewDomainFilter creates a new domain filter
func NewDomainFilter(
	domainIDs map[string]struct{},
	reverseMatch bool,
) DomainFilter {
	if domainIDs == nil {
		domainIDs = make(map[string]struct{})
	}

	return DomainFilter{
		DomainIDs:    domainIDs,
		ReverseMatch: reverseMatch,
	}
}

// Filter returns true if domainID is in the domainID set specified by the filter
func (f DomainFilter) Filter(domainID string) bool {
	_, ok := f.DomainIDs[domainID]
	if f.ReverseMatch {
		ok = !ok
	}
	return ok
}

// Include adds more domainIDs to the domainID set specified by the filter
func (f DomainFilter) Include(domainIDs map[string]struct{}) DomainFilter {
	filter := f.copy()
	for domainID := range domainIDs {
		if !filter.ReverseMatch {
			filter.DomainIDs[domainID] = struct{}{}
		} else {
			delete(filter.DomainIDs, domainID)
		}
	}
	return filter
}

// Exclude removes domainIDs from the domainID set specified by the filter
func (f DomainFilter) Exclude(domainIDs map[string]struct{}) DomainFilter {
	filter := f.copy()
	for domainID := range domainIDs {
		if !filter.ReverseMatch {
			delete(filter.DomainIDs, domainID)
		} else {
			filter.DomainIDs[domainID] = struct{}{}
		}
	}
	return filter
}

// Merge merges the domainID sets specified by two domain filters
func (f DomainFilter) Merge(f2 DomainFilter) DomainFilter {
	// case 1: ReverseMatch field is false for both filters
	if !f.ReverseMatch && !f2.ReverseMatch {
		// union the domainIDs field
		filter := f.copy()
		for domainID := range f2.DomainIDs {
			filter.DomainIDs[domainID] = struct{}{}
		}
		return filter
	}

	// for the following three cases, ReverseMatch field is always true

	// case 2: ReverseMatch field is true for both filters
	if f.ReverseMatch && f2.ReverseMatch {
		// intersect the domainIDs field
		filter := DomainFilter{
			DomainIDs:    make(map[string]struct{}),
			ReverseMatch: true,
		}
		for domainID := range f.DomainIDs {
			if _, ok := f2.DomainIDs[domainID]; ok {
				filter.DomainIDs[domainID] = struct{}{}
			}
		}
		return filter
	}

	// case 3, 4: one of the filters has ReverseMatch equals true
	var filter DomainFilter
	var includeDomainIDs map[string]struct{}
	if f.ReverseMatch {
		filter = f.copy()
		includeDomainIDs = f2.DomainIDs
	} else {
		filter = f2.copy()
		includeDomainIDs = f.DomainIDs
	}

	for domainID := range includeDomainIDs {
		delete(filter.DomainIDs, domainID)
	}

	return filter
}

func (f DomainFilter) copy() DomainFilter {
	domainIDs := make(map[string]struct{})
	for domainID := range f.DomainIDs {
		domainIDs[domainID] = struct{}{}
	}
	return NewDomainFilter(domainIDs, f.ReverseMatch)
}

func covertToDomainIDSet(
	domainIDs []string,
) map[string]struct{} {
	domainIDsMap := make(map[string]struct{})
	for _, domainID := range domainIDs {
		domainIDsMap[domainID] = struct{}{}
	}
	return domainIDsMap
}
