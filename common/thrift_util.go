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

package common

import (
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"
)

// TSerialize is used to serialize thrift TStruct to []byte
func TSerialize(msg thrift.TStruct) (b []byte, err error) {
	return thrift.NewTSerializer().Write(msg)
}

// TSerializeString is used to serialize thrift TStruct to string
func TSerializeString(msg thrift.TStruct) (s string, err error) {
	return thrift.NewTSerializer().WriteString(msg)
}

// TListSerialize is used to serialize list of thrift TStruct to []byte
func TListSerialize(msgs []thrift.TStruct) (b []byte, err error) {
	if msgs == nil {
		return
	}

	t := thrift.NewTSerializer()
	t.Transport.Reset()

	if e := t.Protocol.WriteListBegin(thrift.STRING, len(msgs)); e != nil {
		err = thrift.PrependError("error writing list begin: ", e)
		return
	}

	for _, v := range msgs {
		if e := v.Write(t.Protocol); e != nil {
			err = thrift.PrependError("error writing TStruct: ", e)
			return
		}
	}

	if e := t.Protocol.WriteListEnd(); e != nil {
		err = thrift.PrependError("error writing list end: ", e)
		return
	}

	if err = t.Protocol.Flush(); err != nil {
		return
	}

	if err = t.Transport.Flush(); err != nil {
		return
	}

	b = append(b, t.Transport.Bytes()...)
	return
}

// TDeserialize is used to deserialize []byte to thrift TStruct
func TDeserialize(msg thrift.TStruct, b []byte) (err error) {
	return thrift.NewTDeserializer().Read(msg, b)
}

// TDeserializeString is used to deserialize string to thrift TStruct
func TDeserializeString(msg thrift.TStruct, s string) (err error) {
	return thrift.NewTDeserializer().ReadString(msg, s)
}

// TListDeserialize is used to deserialize []byte to list of thrift TStruct
func TListDeserialize(msgType reflect.Type, b []byte) (msgs []thrift.TStruct, err error) {
	t := thrift.NewTDeserializer()
	err = nil
	if _, err = t.Transport.Write(b); err != nil {
		return
	}

	_, size, e := t.Protocol.ReadListBegin()
	if e != nil {
		err = thrift.PrependError("error reading list begin: ", e)
		return
	}

	msgs = make([]thrift.TStruct, 0, size)
	for i := 0; i < size; i++ {
		msg := reflect.New(msgType).Interface().(thrift.TStruct)
		if e := msg.Read(t.Protocol); e != nil {
			err = thrift.PrependError("error reading TStruct: ", e)
			return
		}
		msgs = append(msgs, msg)
	}

	if e := t.Protocol.ReadListEnd(); e != nil {
		err = thrift.PrependError("error reading list end: ", e)
		return
	}

	return
}
