package tcpraw

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

type fakeFirewall struct {
	exists                          bool
	existsErr, appendErr, deleteErr error
	appended, deleted               bool
}

func (f *fakeFirewall) Exists(string, string, ...string) (bool, error) { return f.exists, f.existsErr }
func (f *fakeFirewall) Append(string, string, ...string) error         { f.appended = true; return f.appendErr }
func (f *fakeFirewall) Delete(string, string, ...string) error         { f.deleted = true; return f.deleteErr }
func TestFirewallFailures(t *testing.T) {
	failure := errors.New("firewall failed")
	for _, f := range []*fakeFirewall{{existsErr: failure}, {appendErr: failure}} {
		if _, err := installFirewallRule(f, []string{"rule"}); !errors.Is(err, failure) {
			t.Fatalf("expected setup error, got %v", err)
		}
	}
	f := &fakeFirewall{deleteErr: failure}
	if err := deleteFirewallRule(f, []string{"rule"}); !errors.Is(err, failure) {
		t.Fatalf("expected delete error, got %v", err)
	}
}
func TestFirewallExistingRuleIsNotOwned(t *testing.T) {
	f := &fakeFirewall{exists: true}
	owned, err := installFirewallRule(f, []string{"rule"})
	if err != nil || owned || f.appended {
		t.Fatalf("existing rule: owned=%v error=%v", owned, err)
	}
}

func TestListenerRuleIncludesAddressAndUniqueOwner(t *testing.T) {
	first := listenerFirewallRule(false, net.ParseIP("192.0.2.1"), 29900, "owner-1")
	second := listenerFirewallRule(false, net.ParseIP("192.0.2.2"), 29900, "owner-2")
	if reflect.DeepEqual(first, second) {
		t.Fatal("different listeners share rule")
	}
	s := strings.Join(first, " ")
	if !strings.Contains(s, "-s 192.0.2.1") || !strings.Contains(s, "owner-1") {
		t.Fatalf("rule lacks address or owner: %s", s)
	}
	wildcard := strings.Join(listenerFirewallRule(false, nil, 29900, "owner-3"), " ")
	if strings.Contains(wildcard, "-s ") {
		t.Fatalf("wildcard should cover all source addresses: %s", wildcard)
	}
}

func TestRawAddressPreservesIPv6Zone(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("fe80::1"), Zone: "eth0", Port: 29900}
	if got := rawDialAddress(addr); got != "fe80::1%eth0" {
		t.Fatalf("got %q", got)
	}
}

func TestZonePreservedInFlowAndListenAddresses(t *testing.T) {
	raw := &net.IPAddr{IP: net.ParseIP("fe80::1"), Zone: "eth0"}
	captured := flowSourceAddress(raw, 29900, "")
	expected := (&net.TCPAddr{IP: raw.IP, Zone: "eth0", Port: 29900}).String()
	if captured.String() != expected {
		t.Fatalf("capture key %s, want %s", captured, expected)
	}
	raw.Zone = ""
	captured = flowSourceAddress(raw, 29900, "eth0")
	if captured.String() != expected {
		t.Fatalf("fallback key %s, want %s", captured, expected)
	}
	listen := rawListenAddress(&net.TCPAddr{IP: raw.IP, Zone: "eth0"})
	if listen.Zone != "eth0" {
		t.Fatalf("listener lost zone: %v", listen)
	}
}

type blockingContextFirewall struct {
	duringAppend bool
	started      chan struct{}
}

func (f *blockingContextFirewall) ExistsContext(ctx context.Context, table, chain string, rule ...string) (bool, error) {
	if !f.duringAppend {
		close(f.started)
		<-ctx.Done()
		return false, ctx.Err()
	}
	return false, nil
}
func (f *blockingContextFirewall) AppendContext(ctx context.Context, table, chain string, rule ...string) error {
	close(f.started)
	<-ctx.Done()
	return ctx.Err()
}
func TestFirewallSetupCancelsBlockedOperations(t *testing.T) {
	for _, stage := range []bool{false, true} {
		t.Run(fmt.Sprint(stage), func(t *testing.T) {
			f := &blockingContextFirewall{duringAppend: stage, started: make(chan struct{})}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_, err := installFirewallRuleContext(ctx, f, []string{"rule"})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error=%v", err)
			}
			select {
			case <-f.started:
			default:
				t.Fatal("operation did not start")
			}
		})
	}
}

func TestCanceledAppendIsMarkedForRollback(t *testing.T) {
	f := &blockingContextFirewall{duringAppend: true, started: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	attempted, err := installFirewallRuleContext(ctx, f, []string{"rule"})
	if !attempted || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("attempted=%v error=%v", attempted, err)
	}
}

type auditSocketCloser struct{ closed bool }

func (c *auditSocketCloser) Close() error { c.closed = true; return nil }

type auditRawConn struct {
	controlErr error
	invoke     bool
}

func (r auditRawConn) Control(f func(uintptr)) error {
	if r.invoke {
		f(123)
	}
	return r.controlErr
}
func (auditRawConn) Read(func(uintptr) bool) error  { return nil }
func (auditRawConn) Write(func(uintptr) bool) error { return nil }
func TestControlSocketReportsControlAndOptionErrors(t *testing.T) {
	want := errors.New("control failed")
	if err := controlSocket(auditRawConn{controlErr: want}, func(uintptr) error { return nil }); !errors.Is(err, want) {
		t.Fatalf("control error: %v", err)
	}
	option := errors.New("option failed")
	if err := controlSocket(auditRawConn{invoke: true}, func(uintptr) error { return option }); !errors.Is(err, option) {
		t.Fatalf("option error: %v", err)
	}
}
func TestFailedAcceptedSocketSetupClosesSocket(t *testing.T) {
	c := &auditSocketCloser{}
	want := errors.New("TTL failed")
	if err := prepareAcceptedSocket(c, func() error { return want }); !errors.Is(err, want) || !c.closed {
		t.Fatalf("error=%v closed=%v", err, c.closed)
	}
}
