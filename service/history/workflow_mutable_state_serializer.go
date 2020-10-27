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

package history

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

func workflowMutableStateToJSON(wms *persistencespb.WorkflowMutableState) string {
	// TODO: call @alexshtin before you dig!
	// This is a _temporarry_ workaround to make JSON serialization work.
	// This code will be removed soon and MutableState should be serialized to proto.

	jsonMarshal := func(v interface{}) string {
		if pb, ok := v.(proto.Message); ok {
			m := jsonpb.Marshaler{}
			var buf bytes.Buffer
			err := m.Marshal(&buf, pb)
			if err != nil {
				return fmt.Sprintf(`{"error": "%s"}`, err.Error())
			}
			return string(buf.Bytes())
		}

		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf(`{"error": "%s"}`, err.Error())
		}
		return string(b)
	}

	var i int
	var sb strings.Builder
	sb.WriteString("{")

	// ActivityInfos
	sb.WriteString(`"ActivityInfos":`)
	sb.WriteString("{")
	i = 1
	for key, item := range wms.ActivityInfos {
		sb.WriteString(fmt.Sprintf(`"%d":%s`, key, jsonMarshal(item)))
		if i != len(wms.ActivityInfos) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("},")

	// TimerInfos
	sb.WriteString(`"TimerInfos":`)
	sb.WriteString("{")
	i = 1
	for key, item := range wms.TimerInfos {
		sb.WriteString(fmt.Sprintf(`"%s":%s`, key, jsonMarshal(item)))
		if i != len(wms.TimerInfos) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("},")

	// ChildExecutionInfos
	sb.WriteString(`"ChildExecutionInfos":`)
	sb.WriteString("{")
	i = 1
	for key, item := range wms.ChildExecutionInfos {
		sb.WriteString(fmt.Sprintf(`"%d":%s`, key, jsonMarshal(item)))
		if i != len(wms.ChildExecutionInfos) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("},")

	// RequestCancelInfos
	sb.WriteString(`"RequestCancelInfos":`)
	sb.WriteString("{")
	i = 1
	for key, item := range wms.RequestCancelInfos {
		sb.WriteString(fmt.Sprintf(`"%d":%s`, key, jsonMarshal(item)))
		if i != len(wms.RequestCancelInfos) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("},")

	// SignalInfos
	sb.WriteString(`"SignalInfos":`)
	sb.WriteString("{")
	i = 1
	for key, item := range wms.SignalInfos {
		sb.WriteString(fmt.Sprintf(`"%d":%s`, key, jsonMarshal(item)))
		if i != len(wms.SignalInfos) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("},")

	// SignalRequestedIDs
	sb.WriteString(fmt.Sprintf(`"SignalRequestedIds":%s,`, jsonMarshal(wms.SignalRequestedIds)))

	// ExecutionInfo
	sb.WriteString(fmt.Sprintf(`"ExecutionInfo":%s,`, jsonMarshal(wms.ExecutionInfo)))

	// ExecutionStats
	sb.WriteString(fmt.Sprintf(`"ExecutionStats":%s,`, jsonMarshal(wms.ExecutionInfo.ExecutionStats)))

	// BufferedEvents
	sb.WriteString(`"BufferedEvents":`)
	sb.WriteString("[")
	i = 1
	for _, item := range wms.BufferedEvents {
		sb.WriteString(jsonMarshal(item))
		if i != len(wms.BufferedEvents) {
			sb.WriteString(",")
		}
		i++
	}
	sb.WriteString("],")

	// VersionHistories
	sb.WriteString(fmt.Sprintf(`"VersionHistories":%s,`, jsonMarshal(wms.ExecutionInfo.VersionHistories)))

	// Checksum
	sb.WriteString(fmt.Sprintf(`"Checksum":%s`, jsonMarshal(wms.Checksum)))

	// Close root object
	sb.WriteString("}")

	return sb.String()
}
