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
