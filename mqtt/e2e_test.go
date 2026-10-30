// SPDX-License-Identifier: MIT

package mqtt_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	sparkplugb "github.com/otfabric/go-sparkplugb"
	"github.com/otfabric/go-sparkplugb/app"
	"github.com/otfabric/go-sparkplugb/edge"
	"github.com/otfabric/go-sparkplugb/host"
	"github.com/otfabric/go-sparkplugb/mqtt"
)

const (
	testUser     = "plant-user"
	testPassword = "s3cret-password"
)

// login is what one CONNECT packet carried.
type login struct {
	username, password string
	hasUser, hasPass   bool
}

// fakeBroker is the part of an MQTT 3.1.1 broker that a client needs to log
// in, subscribe and publish. It accepts testUser with testPassword, and any
// client when open is set.
type fakeBroker struct {
	ln   net.Listener
	open bool

	mu     sync.Mutex
	logins []login
}

func startBroker(t *testing.T, open bool) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &fakeBroker{ln: ln, open: open}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go b.serve(c)
		}
	}()
	return b
}

func (b *fakeBroker) url() string { return "tcp://" + b.ln.Addr().String() }

func (b *fakeBroker) seen() []login {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]login(nil), b.logins...)
}

func (b *fakeBroker) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		first, err := r.ReadByte()
		if err != nil {
			return
		}
		size, err := binary.ReadUvarint(r)
		if err != nil {
			return
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); err != nil {
			return
		}
		var reply []byte
		switch first >> 4 {
		case 1: // CONNECT
			l, ok := parseConnect(body)
			if !ok {
				return
			}
			b.mu.Lock()
			b.logins = append(b.logins, l)
			b.mu.Unlock()
			if !b.open && (l.username != testUser || l.password != testPassword) {
				_, _ = c.Write([]byte{0x20, 2, 0, 5}) // not authorized
				return
			}
			reply = []byte{0x20, 2, 0, 0}
		case 3: // PUBLISH
			if first&0x06 != 0 { // QoS 1: PUBACK of the packet ID after the topic
				n := int(binary.BigEndian.Uint16(body))
				reply = []byte{0x40, 2, body[2+n], body[3+n]}
			}
		case 8: // SUBSCRIBE
			reply = []byte{0x90, 3, body[0], body[1], 1}
		case 12: // PINGREQ
			reply = []byte{0xd0, 0}
		case 14: // DISCONNECT
			return
		}
		if reply != nil {
			if _, err := c.Write(reply); err != nil {
				return
			}
		}
	}
}

// parseConnect reads the variable header and payload of a CONNECT packet.
func parseConnect(b []byte) (login, bool) {
	field := func() (string, bool) {
		if len(b) < 2 || len(b) < 2+int(binary.BigEndian.Uint16(b)) {
			return "", false
		}
		n := int(binary.BigEndian.Uint16(b))
		s := string(b[2 : 2+n])
		b = b[2+n:]
		return s, true
	}
	if _, ok := field(); !ok || len(b) < 4 { // protocol name
		return login{}, false
	}
	flags := b[1]
	b = b[4:]                  // level, flags, keep alive
	if _, ok := field(); !ok { // client ID
		return login{}, false
	}
	if flags&0x04 != 0 { // will topic and message
		if _, ok := field(); !ok {
			return login{}, false
		}
		if _, ok := field(); !ok {
			return login{}, false
		}
	}
	var l login
	if flags&0x80 != 0 {
		l.hasUser = true
		var ok bool
		if l.username, ok = field(); !ok {
			return login{}, false
		}
	}
	if flags&0x40 != 0 {
		l.hasPass = true
		var ok bool
		if l.password, ok = field(); !ok {
			return login{}, false
		}
	}
	return l, true
}

// recordingLogger keeps every line, to look for what must not be there.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) add(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}
func (l *recordingLogger) Debugf(f string, a ...any) { l.add(f, a...) }
func (l *recordingLogger) Infof(f string, a ...any)  { l.add(f, a...) }
func (l *recordingLogger) Warnf(f string, a ...any)  { l.add(f, a...) }
func (l *recordingLogger) Errorf(f string, a ...any) { l.add(f, a...) }

func (l *recordingLogger) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "\n")
}

// role connects one runtime of this module to broker with the given login.
type role func(ctx context.Context, tr mqtt.Transport, broker, user, password string, log sparkplugb.Logger) (close func(), err error)

func roles() map[string]role {
	return map[string]role{
		"edge": func(ctx context.Context, tr mqtt.Transport, broker, user, password string, log sparkplugb.Logger) (func(), error) {
			n, err := edge.New(tr, edge.Config{GroupID: "g", EdgeNodeID: "n", BrokerURL: broker, Username: user, Password: password, Logger: log})
			if err != nil {
				return nil, err
			}
			return func() { _ = n.Close(ctx) }, n.Connect(ctx)
		},
		"host": func(ctx context.Context, tr mqtt.Transport, broker, user, password string, log sparkplugb.Logger) (func(), error) {
			h, err := host.New(tr, host.Config{HostID: "h", BrokerURL: broker, Username: user, Password: password, Logger: log})
			if err != nil {
				return nil, err
			}
			return func() { _ = h.Close(ctx) }, h.Connect(ctx)
		},
		"app": func(ctx context.Context, tr mqtt.Transport, broker, user, password string, log sparkplugb.Logger) (func(), error) {
			a, err := app.New(tr, app.Config{BrokerURL: broker, Username: user, Password: password, Logger: log})
			if err != nil {
				return nil, err
			}
			return func() { _ = a.Close(ctx) }, a.Connect(ctx)
		},
	}
}

func TestE2E_brokerLogin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for name, connect := range roles() {
		t.Run(name+"/accepted", func(t *testing.T) {
			b := startBroker(t, false)
			log := &recordingLogger{}
			closeRole, err := connect(ctx, mqtt.NewPahoTransport(), b.url(), testUser, testPassword, log)
			if err != nil {
				t.Fatal(err)
			}
			closeRole()
			got := b.seen()
			if len(got) != 1 || got[0].username != testUser || got[0].password != testPassword {
				t.Fatalf("logins %+v", got)
			}
			if strings.Contains(log.text(), testPassword) {
				t.Fatalf("the log has the password:\n%s", log.text())
			}
		})
		t.Run(name+"/refused", func(t *testing.T) {
			b := startBroker(t, false)
			log := &recordingLogger{}
			wrong := testPassword + "-wrong"
			closeRole, err := connect(ctx, mqtt.NewPahoTransport(), b.url(), testUser, wrong, log)
			if err == nil {
				closeRole()
				t.Fatal("a wrong password connected")
			}
			if strings.Contains(err.Error(), wrong) || strings.Contains(log.text(), wrong) {
				t.Fatalf("the password leaked: %v\n%s", err, log.text())
			}
		})
		t.Run(name+"/anonymous", func(t *testing.T) {
			b := startBroker(t, true)
			closeRole, err := connect(ctx, mqtt.NewPahoTransport(), b.url(), "", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			closeRole()
			if got := b.seen(); len(got) != 1 || got[0].hasUser || got[0].hasPass {
				t.Fatalf("logins %+v", got)
			}
		})
		t.Run(name+"/password without username", func(t *testing.T) {
			b := startBroker(t, true)
			if _, err := connect(ctx, mqtt.NewPahoTransport(), b.url(), "", testPassword, nil); err == nil || !strings.Contains(err.Error(), "Password requires Username") {
				t.Fatalf("err %v", err)
			}
			if got := b.seen(); len(got) != 0 {
				t.Fatalf("logins %+v", got)
			}
		})
	}
}
