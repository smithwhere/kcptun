package tcpraw

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestResolveTCPAddrContextCancelsDNS(t *testing.T) {
	entered := make(chan struct{}, 2)
	resolver := &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := resolveTCPAddrContext(ctx, resolver, "tcp", "slow.invalid:80"); result <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("DNS lookup did not start")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DNS resolution ignored cancellation")
	}
}

func TestResolveTCPAddrContextNumericAddresses(t *testing.T) {
	for _, tc := range []struct{ network, address string }{
		{"tcp", "127.0.0.1:80"}, {"tcp6", "[::1]:443"}, {"tcp", "[fe80::1%zone0]:42"}, {"tcp", ":80"},
	} {
		got, err := resolveTCPAddrContext(context.Background(), net.DefaultResolver, tc.network, tc.address)
		want, wantErr := net.ResolveTCPAddr(tc.network, tc.address)
		if (err != nil) != (wantErr != nil) || (err == nil && got.String() != want.String()) {
			t.Fatalf("%s %s: got %v, %v; want %v, %v", tc.network, tc.address, got, err, want, wantErr)
		}
	}
}
