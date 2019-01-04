// Copyright (c) 2017 Uber Technologies, Inc.
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

namespace java com.uber.cadence.indexer

include "shared.thrift"

enum MessageType {
  Index
  Delete
}

enum FieldType {
  String
  Int
  Bool
}

struct Field {
  10: optional FieldType type
  20: optional string stringData
  30: optional i64 (js.type = "Long") intData
  40: optional bool boolData
}

struct IndexAttributes {
  10: optional map<string,Field> fields
}

struct Message {
  10: optional MessageType messageType
  20: optional string domainID
  30: optional string workflowID
  40: optional string runID
  50: optional i64 (js.type = "Long") version
  60: optional IndexAttributes indexAttributes
}