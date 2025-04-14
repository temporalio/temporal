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
	"testing"
)

func TestGetDeleteRange(t *testing.T) {
	tests := []struct {
		name         string
		request      DeleteRequest
		expected     DeleteRange
		shouldUpdate bool
	}{
		{
			name: "Delete within range",
			request: DeleteRequest{
				LastIDToDeleteInclusive: 3,
				ExistingMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 5,
				},
			},
			expected: DeleteRange{
				InclusiveMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 3,
				},
				NewMinMessageID:  4,
				MessagesToDelete: 3,
			},
			shouldUpdate: true,
		},
		{
			name: "LastID below range",
			request: DeleteRequest{
				LastIDToDeleteInclusive: 0,
				ExistingMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 5,
				},
			},
			expected:     DeleteRange{},
			shouldUpdate: false,
		},
		{
			name: "LastID exceeds range",
			request: DeleteRequest{
				LastIDToDeleteInclusive: 6,
				ExistingMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 5,
				},
			},
			expected: DeleteRange{
				InclusiveMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 4,
				},
				NewMinMessageID:  6,
				MessagesToDelete: 5,
			},
			shouldUpdate: true,
		},
		{
			name: "Delete the last but one message",
			request: DeleteRequest{
				LastIDToDeleteInclusive: 4,
				ExistingMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 5,
				},
			},
			expected: DeleteRange{
				InclusiveMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 4,
				},
				NewMinMessageID:  5,
				MessagesToDelete: 4,
			},
			shouldUpdate: true,
		},
		{
			name: "Single message in range",
			request: DeleteRequest{
				LastIDToDeleteInclusive: 1,
				ExistingMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 1,
				},
			},
			expected: DeleteRange{
				InclusiveMessageRange: InclusiveMessageRange{
					MinMessageID: 1,
					MaxMessageID: 0,
				},
				NewMinMessageID:  2,
				MessagesToDelete: 1,
			},
			shouldUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, shouldDelete := GetDeleteRange(tt.request)
			if shouldDelete != tt.shouldUpdate {
				t.Errorf("expected %+v for shouldUpdate but got %+v", tt.shouldUpdate, shouldDelete)
			}
			if result != tt.expected {
				t.Errorf("expected %+v but got %+v", tt.expected, result)
			}
		})
	}
}
