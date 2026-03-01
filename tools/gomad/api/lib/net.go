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

package lib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	SIMLANG "go.temporal.io/server/tools/gomad/api/lang"
	SIM "go.temporal.io/server/tools/gomad/runtime"
)

var (
	listenersStateKey = "listenersByAddress"
)

type listenersStateType = map[string]*Listener

func Listen(network, address string) (net.Listener, error) {
	if network != "tcp" {
		panic("net.Listen for non-tcp not implemented")
	}

	normalizedAddr := NormalizeAddr(address)
	if _, found := getListener(normalizedAddr); found {
		SIM.Dbg("🌐️➡️❌", "net:listen", SIM.AnyTag("addr", normalizedAddr))
		return nil, errors.New("connection failed: address already taken")
	}
	SIM.Dbg("🌐️➡️", "net:listen", SIM.AnyTag("addr", normalizedAddr))

	l := &Listener{
		Address:     normalizedAddr,
		newConn:     make(chan net.Conn),
		connections: make(map[net.Conn]net.Conn),
	}
	addListener(normalizedAddr, l)

	return l, nil
}

func Dial(network, address string) (net.Conn, error) {
	var d net.Dialer
	return Dialer_Dial(&d, network, address)
}

func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	d := net.Dialer{Timeout: timeout}
	return Dialer_Dial(&d, network, address)
}

func ListenTCP(network string, tcpAddr *net.TCPAddr) (*TCPListener, error) {
	ip, port := tcpAddr.IP, tcpAddr.Port
	if port == 0 {
		panic("ListenTCP automatic port assignment not implemented")
	}
	if ip == nil {
		ip = net.IPv4(127, 0, 0, 1)
	}
	addr := fmt.Sprintf("%v:%v", ip.String(), port)

	l, err := Listen(network, addr)
	if err != nil {
		return nil, err
	}
	return &TCPListener{l}, nil
}

func LookupHost(host string) (addrs []string, err error) {
	panic("net.LookupHost not implemented yet")
}

func NormalizeAddr(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		panic(err)
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	if host == "::1" {
		host = "127.0.0.1"
	}
	return fmt.Sprintf("%v:%v", host, port)
}

func addListener(addr string, l *Listener) {
	initListeners()
	listeners := SIM.CurrentSimulator().State[listenersStateKey].(listenersStateType)
	listeners[addr] = l
}

func getListener(addr string) (l *Listener, ok bool) {
	initListeners()
	listeners := SIM.CurrentSimulator().State[listenersStateKey].(listenersStateType)
	l, ok = listeners[addr]
	return
}

func initListeners() {
	if _, exists := SIM.CurrentSimulator().State[listenersStateKey]; !exists {
		SIM.CurrentSimulator().State[listenersStateKey] = make(listenersStateType)
	}
}

// ==== DIALER

func Dialer_Dial(d *net.Dialer, network, address string) (net.Conn, error) {
	return Dialer_DialContext(d, context.Background(), network, address)
}

func Dialer_DialContext(d *net.Dialer, ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" {
		panic("net.Dial for non-tcp not implemented")
	}
	if d.Cancel != nil {
		panic("not implemented: Dialer.Cancel")
	}

	fullAddress := network + ":" + NormalizeAddr(address)
	l, found := getListener(fullAddress)
	if !found {
		SIM.Dbg("🌐️⬅️❌", "net:dial", SIM.AnyTag("addr", fullAddress))
		return nil, errors.New("connection failed: no listener found")
	}
	SIM.Dbg("🌐️⬅️️", "net:dial", SIM.AnyTag("addr", fullAddress))

	clientConn, serverConn := Pipe()
	l.connections[serverConn] = clientConn
	SIMLANG.ChanSend(l.newConn, serverConn)

	return clientConn, nil
}

// ==== LISTENER

var _ net.Listener = (*Listener)(nil)

type Listener struct {
	Address     string
	newConn     chan net.Conn
	connections map[net.Conn]net.Conn
}

func (l *Listener) Accept() (net.Conn, error) {
	conn, ok := SIMLANG.ChanRcvOk(l.newConn)
	if ok {
		return conn, nil
	}
	return nil, net.ErrClosed
}

func (l *Listener) Close() error {
	SIMLANG.ChanClose(l.newConn)
	for serverConn, clientConn := range l.connections {
		if err := clientConn.Close(); err != nil {
			return err
		}
		if err := serverConn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Listener) Addr() net.Addr {
	return &net.TCPAddr{}
}

// ==== TCP LISTENER

type TCPListener struct {
	net.Listener
}

func (l *TCPListener) AcceptTCP() (*net.TCPConn, error) {
	panic("TCPListener.AcceptTCP TODO")
}

func (l *TCPListener) File() (f *os.File, err error) {
	panic("TCPListener.File TODO")
}

func (l *TCPListener) SetDeadline(t time.Time) error {
	panic("TCPListener.SetDeadline TODO")
}

func (l *TCPListener) SyscallConn() (syscall.RawConn, error) {
	panic("TCPListener.SyscallConn TODO")
}
