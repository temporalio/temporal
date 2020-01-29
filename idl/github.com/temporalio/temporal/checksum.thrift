// Copyright (c) 2019 Uber Technologies, Inc.
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

include "shared.thrift"

namespace java com.temporalio.temporal

struct MutableStateChecksumPayload {
    10: optional bool cancelRequested
    15: optional i16 state
    16: optional i16 closeStatus

    21: optional i64 (js.type = "Long") lastWriteVersion
    22: optional i64 (js.type = "Long") lastWriteEventID
    23: optional i64 (js.type = "Long") lastFirstEventID
    24: optional i64 (js.type = "Long") nextEventID
    25: optional i64 (js.type = "Long") lastProcessedEventID
    26: optional i64 (js.type = "Long") signalCount

    35: optional i32 decisionAttempt
    36: optional i64 (js.type = "Long") decisionVersion
    37: optional i64 (js.type = "Long") decisionScheduledID
    38: optional i64 (js.type = "Long") decisionStartedID

    45: optional list<i64> pendingTimerStartedIDs
    46: optional list<i64> pendingActivityScheduledIDs
    47: optional list<i64> pendingSignalInitiatedIDs
    48: optional list<i64> pendingReqCancelInitiatedIDs
    49: optional list<i64> pendingChildInitiatedIDs

    55: optional string stickyTaskListName
    56: optional shared.VersionHistories VersionHistories
}
