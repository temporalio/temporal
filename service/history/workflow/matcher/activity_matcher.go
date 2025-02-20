// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matcher

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

/*
MatchActivity matches the given query with the given activity info.
The query should be a valid SQL query with WHERE clause.
The supported fields are:
  - ActivityId
  - ActivityType
  - Attempts
  - BackoffInterval (in seconds)
  - Status
  - LastFailure

Field names are case-sensitive.

The query can have multiple conditions combined with AND/OR.
The query can have conditions on multiple fields.
Date time fields should be in RFC3339 format.
Example query:

	"WHERE ActivityType = 'my_activity' AND Attempts > 10"

Different fields can support different operators:
  - LastFailure: =, !=, starts_with, not starts_with, contains, not contains
  - ActivityType: =, !=, starts_with, not starts_with
  - ActivityId, Status: =, !=
  - Attempts, BackoffInterval: =, !=, >, >=, <, <=

Returns true if the query matches the activity info, false otherwise, or error if the query is invalid.
*/
func MatchActivity(ai *persistencespb.ActivityInfo, query string) (bool, error) {
	evaluator := newActivityMatchEvaluator(ai)
	return evaluator.Evaluate(query)
}
